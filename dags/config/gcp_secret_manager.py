"""
GCP Secret Manager client.

Provides a singleton wrapper around the Secret Manager API, mirroring the
GCPService singleton pattern used for storage/BigQuery access.
"""

import logging
from functools import lru_cache
from typing import Optional

from google.api_core import exceptions as google_exceptions
from google.cloud import secretmanager

logger = logging.getLogger(__name__)


class SecretManagerService:
    """Singleton wrapper around the GCP Secret Manager client."""

    _instance: Optional["SecretManagerService"] = None

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()

    @classmethod
    @lru_cache(maxsize=1)
    def get_instance(cls, project_id: Optional[str] = None) -> "SecretManagerService":
        """
        Get or create the singleton SecretManagerService instance.

        Args:
            project_id: GCP project ID that owns the secrets.

        Returns:
            SecretManagerService: Singleton instance.
        """
        if not project_id:
            raise ValueError("project_id is required to create SecretManagerService")
        return cls(project_id=project_id)

    def get_secret(self, secret_id: str, version: str = "latest") -> str:
        """
        Retrieve a secret's payload from GCP Secret Manager.

        Args:
            secret_id: Name of the secret (not the full resource path).
            version: Secret version to access, defaults to "latest".

        Returns:
            str: The secret payload, decoded as UTF-8.
        """
        secret_path = (
            f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"
        )
        try:
            response = self.client.access_secret_version(name=secret_path)
            return response.payload.data.decode("UTF-8")
        except google_exceptions.NotFound as e:
            logger.error(f"Secret '{secret_id}' not found in project '{self.project_id}'")
            raise ValueError(f"Secret '{secret_id}' not found") from e
        except google_exceptions.PermissionDenied as e:
            logger.error(f"Permission denied accessing secret '{secret_id}': {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to retrieve secret '{secret_id}': {e}")
            raise
