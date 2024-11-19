import uuid
from typing import Any
import pandas as pd

from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3XComBackend(BaseXCom):
    """
    Custom XCom backend for storing and retrieving values in S3.

    The `S3XComBackend` class extends the `BaseXCom` class to provide serialization and deserialization
    methods specific to storing and retrieving values in S3. It supports serializing and deserializing
    pandas DataFrame objects by uploading them to and downloading them from an S3 bucket.

    Attributes:
        PREFIX (str): The prefix used for S3 keys in this XCom backend.
        BUCKET_NAME (str): The name of the S3 bucket for storing XCom values.

    Methods:
        serialize_value(value: Any) -> Any:
            Serializes the provided value, storing DataFrame objects in S3 if detected.

        deserialize_value(result) -> Any:
            Deserializes the provided result, retrieving DataFrame objects from S3 if necessary.
    """

    PREFIX = "xcom_s3://"
    BUCKET_NAME = "sp500-bucket-xcoms"

    @staticmethod
    def serialize_value(value: Any) -> Any:
        """
        Serialize the provided value, storing DataFrame objects in S3 if detected.

        Args:
            value (Any): The value to be serialized.

        Returns:
            Any: The serialized value.
        """
        if isinstance(value, pd.DataFrame):
            hook = S3Hook()
            key = "data_" + str(uuid.uuid4())
            filename = f"{key}.csv"

            value.to_csv(filename)
            hook.load_file(
                filename=filename,
                key=key,
                bucket_name=S3XComBackend.BUCKET_NAME,
                replace=True,
            )
            value = S3XComBackend.PREFIX + key
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        """
        Deserialize the provided result, retrieving DataFrame objects from S3 if necessary.

        Args:
            result (Any): The result to be deserialized.

        Returns:
            Any: The deserialized result.
        """
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(S3XComBackend.PREFIX):
            hook = S3Hook()
            key = result.replace(S3XComBackend.PREFIX, "")
            filename = hook.download_file(
                key=key, bucket_name=S3XComBackend.BUCKET_NAME, local_path="/tmp"
            )
            result = pd.read_csv(filename)
        return result
