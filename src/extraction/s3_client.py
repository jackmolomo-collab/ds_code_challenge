import io
import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config
# from src.extraction.config import config
# Safe and decoupled. Looks directly inside the current folder!
import config



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

    # def select_json(self, key, expression):
    #     response = self.client.select_object_content(
    #         Bucket=self.bucket,
    #         Key=key,
    #         Expression=expression,
    #         ExpressionType="SQL",
    #         InputSerialization={"JSON": {"Type": "DOCUMENT"}},
    #         OutputSerialization={"JSON": {}}
    #     )

    #     records = []

    #     for event in response["Payload"]:
    #         if "Records" in event:
    #             records.append(event["Records"]["Payload"])

    #     return b"".join(records)