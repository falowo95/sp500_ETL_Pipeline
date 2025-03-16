import boto3
from botocore.exceptions import ClientError
import os
from typing import Optional

class AWSConfig:
    """Configuration class for AWS operations using boto3."""

    region_name: str = "eu-west-2"
    session: Optional[boto3.Session] = None

    def __init__(self):
        """Initialize AWS session and clients."""
        self._initialize_session()
        self.secrets_client = self._get_client('secretsmanager')
  
    def _initialize_session(self):
        """Initialize AWS session using environment variables."""
        self.session = boto3.Session(region_name=self.region_name)

    def _get_client(self, service_name: str):
        """Get or create an AWS client."""
        if not self.session:
            raise ValueError("AWS session not initialized")
        return self.session.client(service_name)

    def get_secret(self, secret_name: str) -> str:
        """Retrieve a secret from AWS Secrets Manager."""
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            if 'SecretString' in response:
                return response['SecretString']
            raise ValueError(f"Secret '{secret_name}' does not contain a string value")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            error_messages = {
                'DecryptionFailureException': "Secret decryption failed. Check KMS permissions.",
                'ResourceNotFoundException': f"Secret '{secret_name}' not found.",
                'InvalidRequestException': f"Invalid request for secret '{secret_name}'."
            }
            error_message = error_messages.get(error_code, f"Failed to retrieve secret '{secret_name}'.")
            raise ClientError(f"{error_message} {e}", e.operation_name)