"""
Thin read-only BigQuery wrapper for the dashboard.

Resolves credentials via Application Default Credentials (the Cloud Run
Service's attached runtime service account in production, or a logged-in
gcloud identity locally) — same pattern as the pipeline's own GCPUtils, no
key files. This app never writes to BigQuery; it only reads the marts the
pipeline already produces.
"""
import os
import re
from functools import lru_cache

from google.cloud import bigquery

# Same allow-lists as the pipeline: these strings are interpolated into
# backtick-quoted SQL, where they cannot be bound as query parameters.
_PROJECT_ID = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
_DATASET_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")


def _identifier(value: str, pattern: "re.Pattern[str]", kind: str) -> str:
    if pattern.fullmatch(value) is None:
        raise ValueError(f"Invalid {kind}: {value!r}")
    return value


@lru_cache(maxsize=1)
def get_client() -> bigquery.Client:
    project_id = _identifier(os.environ["GCP_PROJECT_ID"], _PROJECT_ID, "GCP project id")
    return bigquery.Client(project=project_id)


def dataset_ref() -> str:
    project_id = _identifier(os.environ["GCP_PROJECT_ID"], _PROJECT_ID, "GCP project id")
    dataset = _identifier(
        os.environ.get("GCP_BQ_DATASET", "SP_500_DATA"),
        _DATASET_ID,
        "BigQuery dataset name",
    )
    return f"{project_id}.{dataset}"
