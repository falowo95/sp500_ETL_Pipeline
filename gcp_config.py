from google.cloud import storage, bigquery
from google.oauth2 import service_account
import pandas as pd
from typing import Optional, Union, List


class GCPUtils:
    """Utility class for common Google Cloud Platform operations"""

    def __init__(self, credentials_path: str, project_id: str):
        """
        Initialize GCP utilities with credentials and project ID

        Args:
            credentials_path (str): Path to GCP service account credentials JSON
            project_id (str): GCP project ID
        """
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        self.project_id = project_id
        self.storage_client = storage.Client(
            credentials=self.credentials, project=project_id
        )
        self.bq_client = bigquery.Client(
            credentials=self.credentials, project=project_id
        )

    def upload_blob(
        self, bucket_name: str, source_file: str, destination_blob: str
    ) -> None:
        """
        Upload a file to GCS bucket

        Args:
            bucket_name (str): Name of the GCS bucket
            source_file (str): Local file path
            destination_blob (str): Destination path in GCS
        """
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(source_file)

    def download_blob(
        self, bucket_name: str, source_blob: str, destination_file: str
    ) -> None:
        """
        Download a file from GCS bucket

        Args:
            bucket_name (str): Name of the GCS bucket
            source_blob (str): Source path in GCS
            destination_file (str): Local destination file path
        """
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob)
        blob.download_to_filename(destination_file)

    def list_blobs(self, bucket_name: str, prefix: Optional[str] = None) -> List[str]:
        """
        List all blobs in a bucket with optional prefix

        Args:
            bucket_name (str): Name of the GCS bucket
            prefix (str, optional): Filter results to objects beginning with prefix

        Returns:
            List[str]: List of blob names
        """
        blobs = self.storage_client.list_blobs(bucket_name, prefix=prefix)
        return [blob.name for blob in blobs]

    def query_bigquery(self, query: str) -> pd.DataFrame:
        """
        Execute a BigQuery query and return results as DataFrame

        Args:
            query (str): SQL query to execute

        Returns:
            pd.DataFrame: Query results as DataFrame
        """
        return self.bq_client.query(query).to_dataframe()

    def upload_to_bigquery(
        self,
        dataframe: pd.DataFrame,
        table_id: str,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> None:
        """
        Upload DataFrame to BigQuery table

        Args:
            dataframe (pd.DataFrame): DataFrame to upload
            table_id (str): Full table ID (project.dataset.table)
            write_disposition (str): How to handle existing data
                                   ('WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY')
        """
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
        self.bq_client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )

    def delete_blob(self, bucket_name: str, blob_name: str) -> None:
        """
        Delete a blob from GCS bucket

        Args:
            bucket_name (str): Name of the GCS bucket
            blob_name (str): Name of the blob to delete
        """
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
