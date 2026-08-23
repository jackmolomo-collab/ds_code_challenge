import json

from src.extraction.s3_client import S3Client


class H3Extractor:

    def __init__(self):
        self.s3 = S3Client()

    def extract_resolution_8(self, key):

        data = json.loads(
            self.s3.read_geojson(key)
        )

        return [
            feature
            for feature in data["features"]
            if feature["properties"]["resolution"] == 8
        ]