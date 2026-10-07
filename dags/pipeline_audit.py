"""
pipeline_audit.py

Writes one row per DAG run to a `pipeline_runs` BigQuery table. This is the
direct substitute for "glancing at the Airflow UI" in the Cloud Run Jobs
architecture: there is no persistent webserver to check, so run history is
surfaced here instead — via Cloud Logging (this module's own log lines) and
the portfolio dashboard's pipeline-health widget, which reads this table.

It also owns the DAG's final status: `finalize_pipeline_run` is the callable
of the `record_pipeline_run` task (trigger_rule=ALL_DONE, the DAG's only
leaf). Because Airflow derives a DAG run's state from its leaves, that task
must itself fail whenever any upstream task failed — otherwise the run ends
"success", `airflow dags test` exits 0 and the Cloud Run Job failure alert
never fires.
"""

import json
import logging
from datetime import datetime
from typing import Any, List, Mapping, Optional, Sequence, Tuple

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.utils import timezone
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from config.gcp_service import GCPService

logger = logging.getLogger(__name__)

PIPELINE_RUNS_TABLE = "pipeline_runs"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
TASK_STATE_SUCCESS = "success"
# Ordering for the error message: root-cause failures before the
# upstream_failed cascade they caused.
_STATE_PRIORITY = {"failed": 0, "upstream_failed": 1}

EXTRACT_TASK_ID = "extract_data_task"
LOAD_TASK_ID = "ingest_data_into_bigquery"
DBT_TASK_ID = "dbt_build"

PIPELINE_RUNS_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("logical_date", "DATE"),
    bigquery.SchemaField("started_at", "TIMESTAMP"),
    bigquery.SchemaField("finished_at", "TIMESTAMP"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("tickers_attempted", "INTEGER"),
    bigquery.SchemaField("tickers_succeeded", "INTEGER"),
    bigquery.SchemaField("tickers_failed", "INTEGER"),
    bigquery.SchemaField("failed_ticker_list", "STRING"),
    bigquery.SchemaField("rows_loaded", "INTEGER"),
    bigquery.SchemaField("dbt_build_status", "STRING"),
    bigquery.SchemaField("duration_seconds", "FLOAT"),
    bigquery.SchemaField("error_message", "STRING"),
]


class AuditWriteError(RuntimeError):
    """BigQuery rejected the pipeline_runs row (insert_rows_json errors)."""


def compute_run_status(
    task_states: Mapping[str, Optional[str]],
) -> Tuple[str, Optional[str]]:
    """Derive the overall run status from every upstream task's final state.

    Anything other than "success" (failed, upstream_failed, skipped, a
    missing state, ...) makes the run FAILED — nothing in this DAG skips
    legitimately, so a skip can only mean something upstream went wrong.
    An empty mapping is also FAILED: "no evidence of success" is not success.

    Returns:
        (status, error_message) — error_message is None on success and
        otherwise names each non-successful task and its state.
    """
    if not task_states:
        return STATUS_FAILED, "No upstream task states found for this run"

    unsuccessful = sorted(
        (
            (task_id, state)
            for task_id, state in task_states.items()
            if state != TASK_STATE_SUCCESS
        ),
        key=lambda item: (_STATE_PRIORITY.get(str(item[1]), 2), item[0]),
    )
    if not unsuccessful:
        return STATUS_SUCCESS, None

    details = ", ".join(f"{task_id}={state}" for task_id, state in unsuccessful)
    return STATUS_FAILED, f"Upstream task(s) did not succeed: {details}"


def _ensure_pipeline_runs_table(bq_client: bigquery.Client, table_id: str) -> None:
    """Create the pipeline_runs table if it doesn't exist yet. No
    partitioning/clustering here — this table is tiny (one row per day)
    compared to the OHLCV data, so it doesn't need it.

    Only NotFound triggers creation; any other error (permissions, quota,
    transient API failures) propagates rather than being misread as
    "table missing"."""
    try:
        bq_client.get_table(table_id)
    except NotFound:
        bq_client.create_table(bigquery.Table(table_id, schema=PIPELINE_RUNS_SCHEMA))
        logger.info("Created %s", table_id)


def record_pipeline_run(
    dataset_name: str,
    run_id: str,
    logical_date: str,
    started_at: str,
    finished_at: str,
    status: str,
    extract_summary: Optional[Mapping[str, Any]] = None,
    rows_loaded: int = 0,
    dbt_build_status: str = "unknown",
    duration_seconds: float = 0.0,
    error_message: Optional[str] = None,
) -> None:
    """Write one audit row summarizing a single pipeline run.

    Raises:
        AuditWriteError: BigQuery returned row-level insert errors.
        google.api_core.exceptions.GoogleAPICallError: API call failed.
        The caller (finalize_pipeline_run) decides how an audit failure
        interacts with the pipeline's own status.
    """
    summary = extract_summary or {}
    gcp = GCPService.get_instance()
    table_id = f"{gcp.project_id}.{dataset_name}.{PIPELINE_RUNS_TABLE}"
    _ensure_pipeline_runs_table(gcp.bq_client, table_id)

    row = {
        "run_id": run_id,
        "logical_date": logical_date,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "tickers_attempted": summary.get("attempted", 0),
        "tickers_succeeded": summary.get("succeeded", 0),
        "tickers_failed": summary.get("failed", 0),
        "failed_ticker_list": json.dumps(summary.get("failed_tickers", [])),
        "rows_loaded": rows_loaded,
        "dbt_build_status": dbt_build_status,
        "duration_seconds": duration_seconds,
        "error_message": error_message,
    }

    errors = gcp.bq_client.insert_rows_json(table_id, [row])
    if errors:
        logger.error("Failed to write pipeline_runs row for %s: %s", run_id, errors)
        raise AuditWriteError(f"pipeline_runs insert errors: {errors}")
    logger.info("Recorded pipeline run %s: status=%s", run_id, status)


def _upstream_task_instances(dag_run: Any, own_task_id: str) -> List[Any]:
    """Every task instance in the run except the audit task itself (which
    is still "running" while this executes)."""
    return [ti for ti in dag_run.get_task_instances() if ti.task_id != own_task_id]


def _run_timing(
    dag_run: Any, upstream_tis: Sequence[Any]
) -> Tuple[datetime, datetime, float]:
    """(started_at, finished_at, duration_seconds), all UTC-aware.

    started_at is the earliest upstream task start. dag_run.start_date is
    only a fallback: `airflow dags test` (how the Cloud Run Job runs this
    DAG) creates the DagRun with start_date = the logical date, i.e.
    midnight, which would inflate the duration by hours."""
    finished_at = timezone.utcnow()
    task_starts = [ti.start_date for ti in upstream_tis if ti.start_date is not None]
    started_at = min(task_starts) if task_starts else dag_run.start_date
    if started_at is None:
        logger.warning("No start time available for this run; recording zero duration")
        started_at = finished_at
    return started_at, finished_at, (finished_at - started_at).total_seconds()


def _xcom_mapping(ti: Any, task_id: str) -> Mapping[str, Any]:
    """Pull a task's XCom summary, ignoring absent or non-mapping values
    (an upstream task that failed pushes nothing)."""
    value = ti.xcom_pull(task_ids=task_id)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        logger.warning("Ignoring non-mapping XCom from %s: %r", task_id, value)
        return {}
    return value


def _write_audit_row(
    dataset_name: str,
    status: str,
    error_message: Optional[str],
    upstream_tis: Sequence[Any],
    context: Mapping[str, Any],
) -> Optional[Exception]:
    """Write the audit row; return the exception instead of raising so the
    caller can still report the pipeline's real status. Failures are
    logged loudly, with traceback, here."""
    started_at, finished_at, duration = _run_timing(context["dag_run"], upstream_tis)
    dbt_state = next((t.state for t in upstream_tis if t.task_id == DBT_TASK_ID), None)
    ti = context["ti"]
    try:
        record_pipeline_run(
            dataset_name=dataset_name,
            run_id=context["run_id"],
            logical_date=context["ds"],
            started_at=started_at.isoformat(),
            finished_at=finished_at.isoformat(),
            status=status,
            extract_summary=_xcom_mapping(ti, EXTRACT_TASK_ID),
            rows_loaded=_xcom_mapping(ti, LOAD_TASK_ID).get("rows_loaded", 0),
            dbt_build_status=dbt_state or "unknown",
            duration_seconds=duration,
            error_message=error_message,
        )
    except Exception as exc:  # noqa: BLE001 — must never mask pipeline status
        logger.exception(
            "Pipeline audit row write FAILED for run %s", context["run_id"]
        )
        return exc
    return None


def finalize_pipeline_run(dataset_name: str, **context: Any) -> None:
    """Callable for the `record_pipeline_run` task (trigger_rule=ALL_DONE).

    Always attempts to write the audit row, then:
      * any upstream task not successful -> AirflowFailException (no task
        retry: retrying would only duplicate the audit row, the upstream
        failure won't change) — so the DAG run, `airflow dags test` and the
        Cloud Run Job execution all fail;
      * pipeline succeeded but the audit write failed -> AirflowException
        (retryable, so a transient BigQuery error is retried per
        default_args), because a silently missing row would make a good
        run invisible on the dashboard and to alerting.
    """
    upstream_tis = _upstream_task_instances(context["dag_run"], context["ti"].task_id)
    status, error_message = compute_run_status(
        {ti.task_id: ti.state for ti in upstream_tis}
    )
    audit_error = _write_audit_row(
        dataset_name, status, error_message, upstream_tis, context
    )

    if status == STATUS_FAILED:
        suffix = f" (audit row also failed: {audit_error})" if audit_error else ""
        raise AirflowFailException(f"Pipeline run failed. {error_message}{suffix}")
    if audit_error is not None:
        raise AirflowException(
            "Pipeline succeeded but the pipeline_runs audit row could not be written"
        ) from audit_error
