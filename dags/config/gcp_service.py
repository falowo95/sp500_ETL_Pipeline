from functools import lru_cache
from typing import Optional
from config.gcp_config import GCPUtils
import os


class GCPService:
    @classmethod
    @lru_cache(maxsize=1)
    def get_instance(
        cls, credentials_path: Optional[str] = None, project_id: Optional[str] = None
    ) -> GCPUtils:
        """
        Get or create singleton instance of GCPUtils.

        Memoization is provided solely by lru_cache, keyed on the resolved
        (credentials_path, project_id) pair. A redundant `cls._instance` check was
        removed here: it used to short-circuit validation on every call after the
        first success, silently returning a stale instance even when called later
        with missing or different credentials/project_id.

        credentials_path is optional: when not supplied (and not set via
        GOOGLE_APPLICATION_CREDENTIALS), GCPUtils falls back to Application
        Default Credentials rather than requiring a key file.

        Args:
            credentials_path: Optional path to a GCP credentials file
            project_id: GCP project ID (required)

        Returns:
            GCPUtils: Singleton instance of GCP utilities
        """
        if credentials_path is None:
            credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if project_id is None:
            project_id = os.getenv("GCP_PROJECT_ID")

        if not project_id:
            raise ValueError("Missing required GCP project ID")

        return GCPUtils(project_id=project_id, credentials_path=credentials_path)
