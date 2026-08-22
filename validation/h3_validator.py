import json

from src.extraction.s3_client import S3Client


class H3Validator:

    def __init__(self):
        self.s3 = S3Client()

    def validate(self, source):

        data = json.loads(
            self.s3.read_geojson(source)
        )

        features = data["features"]

        total = len(features)
        valid = 0
        errors = []

        for feature in features:

            properties = feature.get("properties", {})

            if (
                properties.get("index")
                and properties.get("resolution") == 8
                and properties.get("centroid_lat") is not None
                and properties.get("centroid_lon") is not None
                and feature.get("geometry")
            ):
                valid += 1
            else:
                errors.append(feature)

        score = valid / total if total else 0

        return {
            "source": source,
            "total": total,
            "valid": valid,
            "invalid": len(errors),
            "score": score,
            "passed": score >= 0.95
        }