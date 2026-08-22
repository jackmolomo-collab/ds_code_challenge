import boto3


class S3Client:
    """Wrapper around the AWS S3 client."""

    def __init__(self, region_name: str):
        self.client = boto3.client(
            "s3",
            region_name=region_name,
        )


