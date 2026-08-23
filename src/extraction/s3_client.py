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

    # =========================================================
    # READ CSV
    # =========================================================

    def read_csv(self, key, **kwargs):
        """
        Read a CSV or GZIP-compressed CSV file from S3.

        Supported:
            file.csv
            file.csv.gz

        The method automatically detects .gz files and
        decompresses them before passing the data to Polars.

        Existing callers can continue using:

            s3.read_csv("sr.csv.gz")

        or:

            s3.read_csv("sr.csv")
        """

        response = self.client.get_object(
            Bucket=self.bucket,
            Key=key,
        )

        raw_data = response["Body"].read()

        # -----------------------------------------------------
        # Handle gzip-compressed CSV
        # -----------------------------------------------------

        if key.lower().endswith(".gz"):

            raw_data = gzip.decompress(
                raw_data
            )

        # -----------------------------------------------------
        # Read CSV using Polars
        # -----------------------------------------------------

        return pl.read_csv(
            io.BytesIO(raw_data),
            **kwargs,
        )

    # =========================================================
    # READ GEOJSON
    # =========================================================

    def read_geojson(self, key):

        response = self.client.get_object(
            Bucket=self.bucket,
            Key=key,
        )

        return response["Body"].read()