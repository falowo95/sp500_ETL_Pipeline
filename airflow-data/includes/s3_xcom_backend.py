from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pandas as pd
import uuid


class S3XComBackend(BaseXCom):
    PREFIX = "xcom_s3://"
    BUCKET_NAME = "sp500-bucket-xcoms"

    @staticmethod
    def serialize_value(value: Any):
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
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(S3XComBackend.PREFIX):
            hook = S3Hook()
            key = result.replace(S3XComBackend.PREFIX, "")
            filename = hook.download_file(
                key=key, bucket_name=S3XComBackend.BUCKET_NAME, local_path="/tmp"
            )
            result = pd.read_csv(filename)
        return result
