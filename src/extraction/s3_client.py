import io

import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config
import yaml


class S3Client:
    """Client for reading public City of Cape Town S3 objects."""

    def __init__(self):
        configpath = "config/aws.yaml"
        with open(configpath, "r") as file:
            aws_config = yaml.safe_load(file)

            self.bucket = aws_config["bucket"]
            self.region = aws_config["region"] 
            self.client = boto3.client("s3",region_name=self.region, config=Config(signature_version=UNSIGNED))
        

  

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