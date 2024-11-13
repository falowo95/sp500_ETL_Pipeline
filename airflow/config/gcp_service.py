from functools import lru_cache
from typing import Optional
from gcp_config import GCPUtils
from config import ETLConfig


class GCPService:
    _instance: Optional[GCPUtils] = None

    @classmethod
    @lru_cache(maxsize=1)
    def get_instance(cls) -> GCPUtils:
        """
        Get or create singleton instance of GCPUtils.
        Uses ETLConfig for configuration management.

        Returns:
            GCPUtils: Singleton instance of GCP utilities
        """
        if cls._instance is None:
            config = ETLConfig()
            credentials_path = config._get_required_env(
                "GOOGLE_APPLICATION_CREDENTIALS"
            )
            project_id = config._get_required_env("GCP_PROJECT_ID")

            cls._instance = GCPUtils(
                credentials_path=credentials_path, project_id=project_id
            )

        return cls._instance
