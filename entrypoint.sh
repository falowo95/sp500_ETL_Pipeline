#!/usr/bin/env bash
set -euo pipefail

# Runs the whole DAG once, in-process, with an ephemeral SQLite metadata DB
# local to this container — nothing persists between Cloud Run Job
# executions. This is the "serverless host for Airflow" pattern: no
# always-on webserver/scheduler/Postgres/Redis anywhere, at the cost of the
# Airflow web UI (compensated for by the pipeline_runs audit table recorded
# at the end of the DAG, surfaced in Cloud Logging and the dashboard).

readonly DAG_ID="SP_500_DATA_PIPELINE_v1"
readonly DATE_PATTERN='^[0-9]{4}-[0-9]{2}-[0-9]{2}$'

# Optional override for backfills / manual re-runs; defaults to today (UTC).
LOGICAL_DATE="${PIPELINE_LOGICAL_DATE:-$(date -u +%Y-%m-%d)}"

# Fail fast on a malformed override, before spending time on the DB migrate,
# rather than letting Airflow surface a less obvious parse error later.
if [[ ! "${LOGICAL_DATE}" =~ ${DATE_PATTERN} ]] || ! date -u -d "${LOGICAL_DATE}" >/dev/null 2>&1; then
    echo "ERROR: PIPELINE_LOGICAL_DATE='${LOGICAL_DATE}' is not a valid YYYY-MM-DD date" >&2
    exit 2
fi

# The migration log is several hundred lines of Alembic noise on a fresh
# SQLite DB every run; keep it out of Cloud Logging unless it fails.
migrate_log="$(mktemp)"
if ! airflow db migrate >"${migrate_log}" 2>&1; then
    echo "ERROR: airflow db migrate failed:" >&2
    cat "${migrate_log}" >&2
    exit 1
fi
rm -f "${migrate_log}"

echo "Running ${DAG_ID} for logical date ${LOGICAL_DATE}"
airflow dags test "${DAG_ID}" "${LOGICAL_DATE}"
