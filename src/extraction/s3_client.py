import io

import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config


class S3Client:
    """Client for reading public City of Cape Town S3 objects."""

    def __init__(
        self,
        bucket: str = "cct-ds-code-challenge-input-data",
        region_name: str = "af-south-1",
    ):
        self.bucket = bucket

        self.client = boto3.client(
            "s3",
            region_name=region_name,
            config=Config(signature_version=UNSIGNED),
        )

    def read_csv(self, key: str, **kwargs) -> pd.DataFrame:
        """Read a CSV or compressed CSV from the public S3 bucket."""

        response = self.client.get_object(
            Bucket=self.bucket,
            Key=key,
        )

        file_stream = response["Body"].read()

        return pd.read_csv(
            io.BytesIO(file_stream),
            compression="gzip",
            **kwargs,
        )