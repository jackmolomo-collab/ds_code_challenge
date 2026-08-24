from dagster import asset, Output, MetadataValue
import time

from src.extraction.s3_client import S3Client
from src.transformations.h3_assignment import H3Assignment
from src.validation.h3_assignment_validator import H3AssignmentValidator


SOURCE_KEY = "sr.csv.gz"
REFERENCE_KEY = "sr_hex.csv.gz"

H3_RESOLUTION = 8
H3_ERROR_THRESHOLD = 0.05


@asset(
    description=(
        "Assign H3 resolution 8 indexes to service requests "
        "and validate the assignments against the reference dataset."
    )
)
def task2_service_requests():

    start = time.perf_counter()

    # --------------------------------------------------------
    # EXTRACT SOURCE DATA
    # --------------------------------------------------------

    s3 = S3Client()

    sr = s3.read_csv(SOURCE_KEY)

    # --------------------------------------------------------
    # H3 ASSIGNMENT
    # --------------------------------------------------------

    assignment = H3Assignment(
        resolution=H3_RESOLUTION
    )

    assigned = assignment.assign(sr)

    # --------------------------------------------------------
    # LOAD REFERENCE DATA
    # --------------------------------------------------------

    reference = s3.read_csv(REFERENCE_KEY)

    # --------------------------------------------------------
    # VALIDATE H3 ASSIGNMENTS
    # --------------------------------------------------------

    validator = H3AssignmentValidator(
        error_threshold=H3_ERROR_THRESHOLD
    )

    validation_result = validator.validate(
        assigned,
        reference,
    )

    duration = round(
        time.perf_counter() - start,
        4,
    )

    # --------------------------------------------------------
    # DAGSTER METADATA
    # --------------------------------------------------------

    return Output(
        value=validation_result,
        metadata={
            "source_records": MetadataValue.int(
                sr.height
            ),
            "reference_records": MetadataValue.int(
                reference.height
            ),
            "matched_records": MetadataValue.int(
                validation_result["matched_records"]
            ),
            "mismatched_records": MetadataValue.int(
                validation_result["mismatched_records"]
            ),
            "match_rate": MetadataValue.float(
                validation_result["match_rate"]
            ),
            "error_rate": MetadataValue.float(
                validation_result["error_rate"]
            ),
            "error_threshold": MetadataValue.float(
                H3_ERROR_THRESHOLD
            ),
            "execution_time_seconds": MetadataValue.float(
                duration
            ),
            "status": MetadataValue.text(
                "passed"
                if validation_result["passed"]
                else "failed"
            ),
        },
    )