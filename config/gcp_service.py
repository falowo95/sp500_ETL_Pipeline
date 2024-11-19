from functools import lru_cache
from typing import Optional
from airflow.config.gcp_config import GCPUtils
import os


class GCPService:
    _instance: Optional[GCPUtils] = None

    @classmethod
    @lru_cache(maxsize=1)
    def get_instance(
        cls, credentials_path: Optional[str] = None, project_id: Optional[str] = None
    ) -> GCPUtils:
        """
        Get or create singleton instance of GCPUtils.

        Args:
            credentials_path: Path to GCP credentials file
            project_id: GCP project ID

        Returns:
            GCPUtils: Singleton instance of GCP utilities
        """
        if cls._instance is None:
            if credentials_path is None:
                credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if project_id is None:
                project_id = os.getenv("GCP_PROJECT_ID")

            if not credentials_path or not project_id:
                raise ValueError("Missing required GCP credentials or project ID")

            cls._instance = GCPUtils(
                credentials_path=credentials_path, project_id=project_id
            )

        return cls._instance
