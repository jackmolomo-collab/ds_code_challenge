import json
import time
from pathlib import Path

from dagster import asset

from src.extraction.s3_client import S3Client
from src.extraction.suburb_extractor import SuburbExtractor
from src.transformations.atlantis_spatial_filter import (
    AtlantisSpatialFilter,
)
from src.transformations.atlantis_request_filter import (
    AtlantisRequestFilter,
)
from src.validation.atlantis_request_validator import (
    AtlantisRequestValidator,
)


OUTPUT_DIR = Path("/opt/dagster/output")

SOURCE_KEY = "sr_hex.csv.gz"

OUTPUT_FILE = (
    OUTPUT_DIR / "task5_1_atlantis_requests.json"
)

VALIDATION_FILE = (
    OUTPUT_DIR / "task5_1_atlantis_validation.json"
)


@asset(
    name="task5_1_atlantis_requests",
    description=(
        "Extracts Atlantis Industrial suburb data, "
        "calculates its centroid, filters service requests "
        "within one arc-minute of the centroid, and validates "
        "the resulting dataset."
    ),
)
def task5_1_atlantis_requests():

    pipeline_start = time.perf_counter()

    # ========================================================
    # 1. EXTRACT OFFICIAL SUBURB DATA
    # ========================================================

    suburb_start = time.perf_counter()

    suburb_extractor = SuburbExtractor()

    suburbs = suburb_extractor.extract()

    suburb_duration = round(
        time.perf_counter() - suburb_start,
        4,
    )

    # ========================================================
    # 2. IDENTIFY ATLANTIS SUBURB
    # ========================================================

    spatial_filter = AtlantisSpatialFilter()

    atlantis_suburbs = (
        spatial_filter.identify_atlantis_suburbs(
            suburbs
        )
    )

    if atlantis_suburbs.empty:
        raise ValueError(
            "No Atlantis suburb was identified."
        )

    # ========================================================
    # 3. CALCULATE CENTROID
    # ========================================================

    centroid_data = (
        spatial_filter.calculate_centroids(
            atlantis_suburbs
        )
    )

    centroid = centroid_data.geometry.iloc[0].centroid

    centroid_latitude = centroid.y
    centroid_longitude = centroid.x

    # ========================================================
    # 4. EXTRACT SERVICE REQUEST DATA
    # ========================================================

    s3_start = time.perf_counter()

    s3_client = S3Client()

    service_requests = s3_client.read_csv(
        SOURCE_KEY
    )

    s3_duration = round(
        time.perf_counter() - s3_start,
        4,
    )

    # ========================================================
    # 5. FILTER REQUESTS
    # ========================================================

    filter_start = time.perf_counter()

    request_filter = AtlantisRequestFilter()

    filtered_requests = request_filter.filter_requests(
        service_requests,
        centroid_latitude,
        centroid_longitude,
    )

    filter_duration = round(
        time.perf_counter() - filter_start,
        4,
    )

    # ========================================================
    # 6. VALIDATE RESULT
    # ========================================================

    validation_start = time.perf_counter()

    validator = AtlantisRequestValidator()

    validation_result = validator.validate(
        service_requests,
        filtered_requests,
    )

    validation_duration = round(
        time.perf_counter() - validation_start,
        4,
    )

    if not validation_result["passed"]:
        raise ValueError(
            "Task 5.1 validation failed: "
            f"{validation_result}"
        )

    # ========================================================
    # 7. PREPARE OUTPUT
    # ========================================================

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_records = (
        filtered_requests
        .to_dicts()
    )

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            output_records,
            file,
            indent=2,
            default=str,
        )

    # ========================================================
    # 8. WRITE VALIDATION RESULT
    # ========================================================

    validation_output = {
        "task": "task5_1_atlantis_requests",
        "status": "success",
        "source": SOURCE_KEY,
        "atlantis_suburbs": (
            atlantis_suburbs[
                "OFC_SBRB_NAME"
            ].tolist()
        ),
        "centroid": {
            "latitude": centroid_latitude,
            "longitude": centroid_longitude,
        },
        "validation": validation_result,
        "metrics": {
            "suburb_extraction_seconds": suburb_duration,
            "service_request_extraction_seconds": (
                s3_duration
            ),
            "filter_seconds": filter_duration,
            "validation_seconds": validation_duration,
            "total_duration_seconds": round(
                time.perf_counter()
                - pipeline_start,
                4,
            ),
        },
        "output": str(OUTPUT_FILE),
    }

    with open(
        VALIDATION_FILE,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            validation_output,
            file,
            indent=2,
            default=str,
        )

    # ========================================================
    # 9. RETURN DAGSTER ASSET RESULT
    # ========================================================

    return {
        "output": str(OUTPUT_FILE),
        "validation": str(VALIDATION_FILE),
        "records": filtered_requests.height,
        "validation_passed": validation_result[
            "passed"
        ],
    }