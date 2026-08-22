import io
import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config
from src.extraction.config import config


class S3Client:

    def __init__(self):
        self.bucket = config["s3"]["bucket"]

        self.client = boto3.client(
            "s3",
            region_name=config["aws"]["region"],
            config=Config(signature_version=UNSIGNED),
        )

    def read_csv(self, key, **kwargs):
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=key
        )

        return pd.read_csv(
            io.BytesIO(response["Body"].read()),
            compression="gzip",
            **kwargs
        )


    def read_geojson(self, key):
        response = self.client.get_object(
        Bucket=self.bucket,
        Key=key)

        return response["Body"].read()