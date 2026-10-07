"""
GCP Secret Manager client.

Provides a singleton wrapper around the Secret Manager API, mirroring the
GCPService singleton pattern used for storage/BigQuery access.
"""

import logging
import re
from functools import lru_cache
from typing import Optional

from google.api_core import exceptions as google_exceptions
from google.cloud import secretmanager

from config.validation import GCP_PROJECT_PATTERN

logger = logging.getLogger(__name__)

# Secret Manager ids start with a letter, then letters, digits, "_" or "-",
# at most 255 characters. Versions are "latest" or a positive integer.
# Both are interpolated into the resource name, so anything else is rejected
# before it can retarget the request (for example "a/versions/1").
_SECRET_ID_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,254}$")
_SECRET_VERSION_PATTERN = re.compile(r"^(latest|[1-9][0-9]{0,19})$")


class SecretManagerService:
    """Singleton wrapper around the GCP Secret Manager client.

    Memoization is only lru_cache on get_instance. There is no cls._instance
    shortcut — that used to return the first client even when a later call
    passed a different project id.
    """

    def __init__(self, project_id: str):
        if GCP_PROJECT_PATTERN.fullmatch(project_id or "") is None:
            raise ValueError(f"Invalid GCP project id: {project_id!r}")
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
            str: The secret payload, decoded as UTF-8, with surrounding
            whitespace removed. A here-string used to create the secret
            appends a newline, which would otherwise be sent as part of the
            Tiingo token.
        """
        if _SECRET_ID_PATTERN.fullmatch(secret_id or "") is None:
            raise ValueError(f"Invalid secret id: {secret_id!r}")
        if _SECRET_VERSION_PATTERN.fullmatch(version or "") is None:
            raise ValueError(f"Invalid secret version: {version!r}")

        secret_path = (
            f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"
        )
        try:
            response = self.client.access_secret_version(name=secret_path)
            payload = response.payload.data.decode("UTF-8").strip()
        except google_exceptions.NotFound as e:
            logger.error("Secret '%s' not found in project '%s'", secret_id, self.project_id)
            raise ValueError(f"Secret '{secret_id}' not found") from e
        except google_exceptions.PermissionDenied:
            logger.error("Permission denied accessing secret '%s'", secret_id)
            raise
        except Exception:
            logger.error("Failed to retrieve secret '%s'", secret_id)
            raise
        if not payload:
            raise ValueError(f"Secret '{secret_id}' is empty")
        return payload
