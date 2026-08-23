import gzip
import io

import boto3
import polars as pl
from botocore import UNSIGNED
from botocore.config import Config

from src.extraction.config import config


class S3Client:

    def __init__(self):

        self.bucket = config["s3"]["bucket"]

        self.client = boto3.client(
            "s3",
            region_name=config["aws"]["region"],
            config=Config(
                signature_version=UNSIGNED
            ),
        )

    def read_csv(self, key, **kwargs):

        response = self.client.get_object(
            Bucket=self.bucket,
            Key=key,
        )

        data = response["Body"].read()

        if key.lower().endswith(".gz"):

            data = gzip.decompress(data)

        return pl.read_csv(
            io.BytesIO(data),
            **kwargs,
        )

    def read_geojson(self, key):

        response = self.client.get_object(
            Bucket=self.bucket,
            Key=key,
        )

        return response["Body"].read()