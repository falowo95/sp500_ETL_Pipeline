"""
AWS Configuration and Operations Class

This module provides a class for managing AWS operations including S3 and Redshift connections.
It handles authentication and provides methods for common AWS operations.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any, Union
import boto3
from botocore.exceptions import ClientError
import json
import base64
from datetime import datetime
from pathlib import Path
import pandas as pd


@dataclass
class AWSConfig:
    """Configuration class for AWS operations.

    This class manages AWS connections and operations, handling authentication
    and providing methods for S3 and Redshift interactions.

    Attributes:
        region_name: AWS region to connect to
        profile_name: AWS credential profile to use
        secret_name: Name of secret in AWS Secrets Manager
        iam_role: Optional IAM role to assume
    """

    region_name: str
    profile_name: Optional[str] = None
    secret_name: Optional[str] = None
    iam_role: Optional[str] = None

    def __post_init__(self):
        """Initialize AWS session and clients after instance creation."""
        # Initialize base session
        self.session = boto3.Session(
            profile_name=self.profile_name, region_name=self.region_name
        )

        # Assume role if specified
        if self.iam_role:
            credentials = self._get_role_credentials(self.iam_role)
            self.session = boto3.Session(
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                region_name=self.region_name,
            )

        # Initialize clients
        self.s3_client = self.session.client("s3")
        self.s3_resource = self.session.resource("s3")
        self.secrets_client = self.session.client("secretsmanager")
        self._credentials: Optional[Dict[str, Any]] = None

    def _get_role_credentials(self, role_arn: str) -> Dict[str, Any]:
        """Retrieve credentials for a specific IAM role.

        Args:
            role_arn: The ARN of the IAM role to assume

        Returns:
            Dict containing the role credentials
        """
        sts_client = self.session.client("sts")
        assumed_role = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName="AWSConfigSession"
        )
        return assumed_role["Credentials"]

    def get_secret(self) -> Dict[str, Any]:
        """Retrieve secret from AWS Secrets Manager and convert to dict.

        Returns:
            Dict containing the secret key/value pairs

        Raises:
            ClientError: If secret retrieval fails
            ValueError: If secret name not configured
        """
        if not self.secret_name:
            raise ValueError("Secret name not configured")

        try:
            response = self.secrets_client.get_secret_value(SecretId=self.secret_name)
            return json.loads(response["SecretString"])
        except ClientError as e:
            raise ClientError(f"Failed to retrieve secret {self.secret_name}: {str(e)}")

    def download_from_s3(self, bucket: str, s3_key: str, local_path: str) -> None:
        """Download a file from S3 bucket.

        Args:
            bucket: S3 bucket name
            s3_key: Source path in S3
            local_path: Local destination path

        Raises:
            ClientError: If download fails
        """
        try:
            self.s3_client.download_file(Bucket=bucket, Key=s3_key, Filename=local_path)
        except ClientError as e:
            raise ClientError(f"Failed to download file from S3: {str(e)}")

    def get_redshift_connection(
        self, database: str, user: str, password: str, host: str, port: int = 5439
    ):
        """Get connection to Redshift cluster.

        Args:
            database: Redshift database name
            user: Database username
            password: Database password
            host: Redshift host address
            port: Redshift port number

        Returns:
            Redshift connection object
        """
        try:
            redshift = self.session.client("redshift")
            # Return connection parameters that can be used with psycopg2
            return {
                "dbname": database,
                "user": user,
                "password": password,
                "host": host,
                "port": port,
            }
        except ClientError as e:
            raise ClientError(f"Failed to establish Redshift connection: {str(e)}")

    def list_s3_objects(self, bucket: str, prefix: str = "") -> list:
        """List objects in S3 bucket with given prefix.

        Args:
            bucket: S3 bucket name
            prefix: Prefix to filter objects

        Returns:
            List of object keys

        Raises:
            ClientError: If listing fails
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except ClientError as e:
            raise ClientError(f"Failed to list S3 objects: {str(e)}")

    def get_caller_identity(self) -> Dict[str, Any]:
        """Get the identity of the current AWS caller.

        Returns:
            Dict containing caller identity information
        """
        sts_client = self.session.client("sts")
        return sts_client.get_caller_identity()

    @staticmethod
    def datetime_from_aws_timestamp(aws_timestamp: int) -> datetime:
        """Convert AWS timestamp into datetime format.

        Args:
            aws_timestamp: AWS timestamp (UNIX timestamp * 1000)

        Returns:
            datetime formatted AWS timestamp
        """
        return datetime.utcfromtimestamp(aws_timestamp / 1000)

    def upload_s3_file(
        self,
        local_path: Union[Path, str],
        s3_bucket: str,
        s3_name: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Upload local file to S3 bucket.

        Args:
            local_path: Local path of file to upload
            s3_bucket: Name of S3 bucket
            s3_name: Name of file in S3 bucket (defaults to local filename)
            extra_args: Extra args such as ContentEncoding

        Returns:
            Boolean indicating successful upload
        """
        local_path = local_path if isinstance(local_path, Path) else Path(local_path)
        s3_name = s3_name if s3_name is not None else local_path.name

        try:
            self.s3_client.upload_file(
                str(local_path), s3_bucket, s3_name, ExtraArgs=extra_args
            )
            return True
        except ClientError as e:
            raise ClientError(f"Failed to upload file to S3: {str(e)}")

    def delete_s3_file(self, s3_bucket: str, s3_name: str) -> bool:
        """Delete file from S3.

        Args:
            s3_bucket: Name of S3 bucket
            s3_name: Name of file in S3 bucket

        Returns:
            Boolean indicating successful deletion
        """
        try:
            self.s3_client.delete_object(Bucket=s3_bucket, Key=s3_name)
            return True
        except ClientError as e:
            raise ClientError(f"Failed to delete S3 file: {str(e)}")

    def get_s3_file_metadata(self, s3_bucket: str, file_key: str) -> Dict[str, Any]:
        """Retrieve metadata about a specified file in an S3 bucket.

        Args:
            s3_bucket: Name of S3 bucket
            file_key: The name of the file

        Returns:
            Dict containing file metadata
        """
        try:
            metadata = self.s3_client.head_object(Bucket=s3_bucket, Key=file_key)
            return metadata
        except ClientError as e:
            raise ClientError(f"Failed to get S3 file metadata: {str(e)}")

    def get_log_stream_dataframe(self, log_group_name: str) -> pd.DataFrame:
        """Get DataFrame of log streams in the Cloudwatch log group.

        Args:
            log_group_name: Name of Cloudwatch log group

        Returns:
            DataFrame of log streams and their creation times
        """
        logs_client = self.session.client("logs")
        kwargs = {
            "logGroupName": log_group_name,
            "limit": 50,
        }

        log_stream_rows = []
        while True:
            response = logs_client.describe_log_streams(**kwargs)
            log_stream_rows.extend(response["logStreams"])

            if "nextToken" not in response:
                break
            kwargs["nextToken"] = response["nextToken"]

        log_stream_df = pd.DataFrame(log_stream_rows)
        log_stream_df = log_stream_df.loc[:, ["creationTime", "logStreamName"]]
        log_stream_df["creationTime"] = log_stream_df["creationTime"].apply(
            self.datetime_from_aws_timestamp
        )
        log_stream_df.rename(
            columns={
                "creationTime": "creation_time",
                "logStreamName": "log_stream_name",
            },
            inplace=True,
        )
        log_stream_df.sort_values(
            by="creation_time",
            ascending=False,
            ignore_index=True,
            inplace=True,
        )

        return log_stream_df

    def get_log_dataframe(
        self,
        log_group_name: str,
        log_stream_name: str,
    ) -> pd.DataFrame:
        """Get DataFrame of log entries for specific log group and stream.

        Args:
            log_group_name: Name of Cloudwatch log group
            log_stream_name: Name of log stream

        Returns:
            DataFrame of log entries and their timestamps
        """
        logs_client = self.session.client("logs")
        kwargs = {
            "logGroupName": log_group_name,
            "logStreamNames": [log_stream_name],
            "limit": 10000,
        }

        log_rows = []
        while True:
            response = logs_client.filter_log_events(**kwargs)
            log_rows.extend(response["events"])

            if "nextToken" not in response:
                break
            kwargs["nextToken"] = response["nextToken"]

        log_df = pd.DataFrame(log_rows)
        log_df = log_df.loc[:, ["timestamp", "message"]]
        log_df["timestamp"] = log_df["timestamp"].apply(
            self.datetime_from_aws_timestamp
        )
        log_df.rename(columns={"timestamp": "utc_timestamp"}, inplace=True)

        return log_df

    def get_latest_log_dataframe(self, log_group_name: str) -> pd.DataFrame:
        """Get DataFrame of log entries for latest log stream in log group.

        Args:
            log_group_name: Name of Cloudwatch log group

        Returns:
            DataFrame of log entries and their timestamps
        """
        log_stream_df = self.get_log_stream_dataframe(log_group_name=log_group_name)
        latest_log_stream_name = log_stream_df["log_stream_name"][0]
        return self.get_log_dataframe(
            log_group_name=log_group_name,
            log_stream_name=latest_log_stream_name,
        )
