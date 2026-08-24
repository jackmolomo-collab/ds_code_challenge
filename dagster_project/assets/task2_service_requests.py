
from dagster import asset
import time


from src.extraction.s3_client import S3Client
from src.transformations.h3_assignment import H3Assignment
from src.validation.h3_assignment_validator import (
    H3AssignmentValidator,
)
from src.outputs.result_writer import ResultWriter


# ============================================================
# CONFIGURATION
# ============================================================

SOURCE_KEY = "sr.csv.gz"

REFERENCE_KEY = "sr_hex.csv.gz"

OUTPUT_DIR = "/opt/dagster/output"

H3_RESOLUTION = 8

H3_ERROR_THRESHOLD = 0.05


# ============================================================
# TASK 2
# SERVICE REQUEST H3 ASSIGNMENT
# ============================================================

@asset
def task2_service_requests():

    start = time.perf_counter()

    print("=" * 60)
    print("TASK 2 - SERVICE REQUEST H3 ASSIGNMENT")
    print("=" * 60)

    # ========================================================
    # 1. CREATE CLIENTS
    # ========================================================

    s3 = S3Client()

    writer = ResultWriter(
        OUTPUT_DIR
    )

    # ========================================================
    # 2. READ SERVICE REQUEST DATA
    # ========================================================

    print()
    print("Reading service requests...")

    sr = s3.read_csv(
        SOURCE_KEY
    )

    print(
        f"Service request records: {sr.height:,}"
    )

    # ========================================================
    # 3. ASSIGN H3
    # ========================================================

    print()
    print(
        f"Assigning H3 resolution {H3_RESOLUTION}..."
    )

    h3_assignment = H3Assignment(
        H3_RESOLUTION
    )

    assigned = h3_assignment.assign(
        sr
    )

    print(
        "H3 assignment completed."
    )

    # ========================================================
    # 4. READ REFERENCE DATA
    # ========================================================

    print()
    print(
        "Reading H3 reference data..."
    )

    reference = s3.read_csv(
        REFERENCE_KEY
    )

    print(
        f"Reference records: {reference.height:,}"
    )

    # ========================================================
    # 5. VALIDATE H3 ASSIGNMENTS
    # ========================================================

    print()
    print(
        "Validating H3 assignments..."
    )

    validator = H3AssignmentValidator(
        H3_ERROR_THRESHOLD
    )

    validation_result = validator.validate(
        assigned,
        reference,
    )

    print()
    print(
        "H3 assignment validation completed."
    )

    # ========================================================
    # 6. WRITE SERVICE REQUEST OUTPUT
    # ========================================================

    print()
    print(
        "Writing service request output..."
    )

    output_path = writer.write_dataframe(
        assigned,
        "task2_service_requests.json",
    )

    print(
        f"Service request output: {output_path}"
    )

    # ========================================================
    # 7. WRITE VALIDATION RESULT
    # ========================================================

    validation_output_path = writer.write_json(
        validation_result,
        "task2_h3_assignment_validation.json",
    )

    print(
        "Validation output: "
        f"{validation_output_path}"
    )

    # ========================================================
    # 8. BUILD METRICS
    # ========================================================

    duration = round(
        time.perf_counter() - start,
        4,
    )

    metrics = {
        "task": "task2_service_requests",
        "status": "success",
        "duration_seconds": duration,
        "input_records": sr.height,
        "reference_records": reference.height,
        "output_records": assigned.height,
        "h3_resolution": H3_RESOLUTION,
        "h3_error_threshold": H3_ERROR_THRESHOLD,
        "h3_match_rate": validation_result[
            "match_rate"
        ],
        "h3_error_rate": validation_result[
            "error_rate"
        ],
        "h3_validation_passed": validation_result[
            "passed"
        ],
        "output_path": output_path,
        "validation_output_path": (
            validation_output_path
        ),
    }

    # ========================================================
    # 9. WRITE TASK 2 METRICS
    # ========================================================

    metrics_output_path = writer.write_json(
        metrics,
        "task2_service_requests_metrics.json",
    )

    print()
    print("=" * 60)
    print("TASK 2 COMPLETED")
    print("=" * 60)

    print(
        f"Input records:       {sr.height:,}"
    )

    print(
        f"Output records:      {assigned.height:,}"
    )

    print(
        "H3 match rate:       "
        f"{validation_result['match_rate']:.4%}"
    )

    print(
        "H3 error rate:       "
        f"{validation_result['error_rate']:.4%}"
    )

    print(
        "Validation passed:   "
        f"{validation_result['passed']}"
    )

    print(
        f"Duration:            {duration:.4f}s"
    )

    print(
        f"Output:              {output_path}"
    )

    print(
        f"Validation:          "
        f"{validation_output_path}"
    )

    print(
        f"Metrics:             "
        f"{metrics_output_path}"
    )

    return {
        "output_path": output_path,
        "validation_output_path": (
            validation_output_path
        ),
        "metrics_output_path": (
            metrics_output_path
        ),
        "records": assigned.height,
        "validation": validation_result,
    }

