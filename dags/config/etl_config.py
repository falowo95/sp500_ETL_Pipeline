from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional
from pathlib import Path
import os
from functools import cached_property
from config.gcp_config import GCPUtils
from config.gcp_service import GCPService


@dataclass(frozen=True)
class ETLConfig:
    """Configuration class for ETL process.

    This class contains all configuration parameters needed for the ETL pipeline,
    including Airflow settings, API credentials, and data paths.

    Attributes:
        owner: The owner of the Airflow DAG
        start_date: The start date for the Airflow DAG
        email: List of email addresses for notifications
        email_on_failure: Whether to send email on task failure
        email_on_retry: Whether to send email on task retry
        depends_on_past: Whether tasks depend on past executions
        retries: Number of retries for failed tasks
        retry_delay: Delay between retries
    """

    # Airflow configs
    owner: Optional[str] = None
    start_date: Optional[datetime] = None
    email: List[str] = field(default_factory=list)
    email_on_failure: bool = False
    email_on_retry: bool = False
    depends_on_past: bool = False
    retries: int = 1
    retry_delay: timedelta = field(default_factory=lambda: timedelta(minutes=5))

    # Data configs
    file_name: str = "SP_500_DATA"
    data_start_date: str = "2015-01-01"
    data_end_date: str = "2021-01-01"

    @cached_property
    def tiingo_api_key(self) -> str:
        """Get Tiingo API key from environment variables."""
        return self._get_required_env("TIINGO_API_KEY")

    @cached_property
    def bucket_name(self) -> str:
        """Get GCP bucket name from environment variables."""
        return self._get_required_env("GCP_GCS_BUCKET")

    @property
    def dataset_name(self) -> str:
        """Get dataset name."""
        return self.file_name

    @property
    def table_name(self) -> str:
        """Get table name."""
        return f"{self.dataset_name}_table"

    @property
    def source_file_path_local(self) -> Path:
        """Get local source file path."""
        return Path(f"{self.file_name}.csv")

    @property
    def destination_blob_path(self) -> str:
        """Get GCS destination blob path."""
        return f"input-data/{self.file_name}.csv"

    @property
    def base_gcs_path(self) -> str:
        """Get base GCS path."""
        return f"gs://{self.bucket_name}"

    @property
    def gcs_input_data_path(self) -> str:
        """Get GCS input data path."""
        return f"{self.base_gcs_path}/input-data/{self.file_name}.csv"

    @property
    def gcs_output_data_path(self) -> str:
        """Get GCS output data path."""
        return f"{self.base_gcs_path}/transformed-data/"

    @property
    def csv_uri(self) -> str:
        """Get CSV URI pattern for transformed data."""
        return f"{self.base_gcs_path}/transformed-data/*.csv"

    @cached_property
    def gcp_project_id(self) -> str:
        """Get GCP project ID from environment variables."""
        return self._get_required_env("GCP_PROJECT_ID")

    @cached_property
    def gcp_credentials_path(self) -> str:
        """Get GCP credentials path from environment variables."""
        return self._get_required_env("GOOGLE_APPLICATION_CREDENTIALS")

    @cached_property
    def gcp_utils(self) -> GCPUtils:
        """Get or create GCPUtils instance."""
        return GCPService.get_instance(
            credentials_path=self.gcp_credentials_path, project_id=self.gcp_project_id
        )

    @staticmethod
    def _get_required_env(env_var: str) -> str:
        """Get required environment variable or raise error if not found.

        Args:
            env_var: Name of the environment variable

        Returns:
            The value of the environment variable

        Raises:
            ValueError: If the environment variable is not set
        """
        value = os.getenv(env_var)
        if not value:
            raise ValueError(f"Required environment variable '{env_var}' is not set")
        return value
