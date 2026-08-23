import time
from pathlib import Path

import polars as pl
from dagster import asset

from src.extraction.s3_client import S3Client
from src.validation.schema_validator import SchemaValidator
from src.transformations.h3_assignment import H3Assignment
from src.monitoring.pipeline_metrics import PipelineMetrics
from src.outputs.result_writer import ResultWriter


@asset
def task1_h3():

    pipeline_start = time.perf_counter()

    # =========================================================
    # Configuration
    # =========================================================

    # Docker path mapped to:
    # C:\Projects\City of Cape Town\Project\ds_code_challenge\output
    output_dir = Path("/opt/dagster/output")

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    metrics = PipelineMetrics(
        output_dir=output_dir
    )

    writer = ResultWriter(
        output_dir=output_dir
    )

    s3 = S3Client()

    # =========================================================
    # 1. Load service requests
    # =========================================================

    start = metrics.start()

    sr = s3.read_csv(
        "sr.csv.gz"
    )

    metrics.record(
        operation="load_service_requests",
        start_time=start,
        records=len(sr),
    )

    # =========================================================
    # 2. Validate service request schema
    # =========================================================

    start = metrics.start()

    # schema_path = (
    #     Path("/opt/dagster/config")
    #     / "schemas"
    #     / "service_request_schema.yaml"
    # )

    schema_path = (
    Path("/opt/dagster/config")
    / "schemas"
    / "service_request_schema.yaml")

    schema_validator = SchemaValidator(
        schema_path=schema_path
    )

    schema_result = schema_validator.validate(
        sr
    )

    writer.write_json(
        schema_result,
        "task1_schema_validation.json",
    )

    metrics.record(
        operation="validate_service_request_schema",
        start_time=start,
        records=len(sr),
        status=(
            "success"
            if schema_result["passed"]
            else "failed"
        ),
    )

    if not schema_result["passed"]:
        raise ValueError(
            "Service request schema validation failed."
        )

    # =========================================================
    # 3. Assign H3
    # =========================================================

    start = metrics.start()

    h3_assignment = H3Assignment(
        resolution=8
    )

    result = h3_assignment.assign(
        sr
    )

    metrics.record(
        operation="assign_h3_resolution_8",
        start_time=start,
        records=len(result),
    )

    # =========================================================
    # 4. Validate missing-coordinate behaviour
    # =========================================================

    start = metrics.start()

    missing_coordinates = (
        sr
        .select(
            (
                pl.col("latitude").is_null()
                |
                pl.col("longitude").is_null()
            )
            .sum()
            .alias("missing_coordinates")
        )
        .item()
    )

    zero_h3 = (
        result
        .filter(
            pl.col("h3_level8_index") == "0"
        )
        .height
    )

    if missing_coordinates != zero_h3:
        raise ValueError(
            "H3 missing-coordinate validation failed: "
            f"{missing_coordinates} records have missing "
            f"coordinates but {zero_h3} records have H3 index 0."
        )

    metrics.record(
        operation="validate_missing_coordinates",
        start_time=start,
        records=missing_coordinates,
        status="success",
    )

    # =========================================================
    # 5. Load sr_hex reference dataset
    # =========================================================

    start = metrics.start()

    sr_hex = s3.read_csv(
        "sr_hex.csv.gz"
    )

    metrics.record(
        operation="load_sr_hex_reference",
        start_time=start,
        records=len(sr_hex),
    )

    # =========================================================
    # 6. Validate row counts
    # =========================================================

    start = metrics.start()

    if len(result) != len(sr_hex):
        raise ValueError(
            "Row count mismatch between generated "
            "service requests and sr_hex reference: "
            f"{len(result)} != {len(sr_hex)}"
        )

    metrics.record(
        operation="validate_row_count",
        start_time=start,
        records=len(result),
        status="success",
    )

    # =========================================================
    # 7. Validate H3 against sr_hex
    # =========================================================

    start = metrics.start()

    comparison = (
        result
        .select(
            [
                "notification_number",
                "h3_level8_index",
            ]
        )
        .join(
            sr_hex.select(
                [
                    "notification_number",
                    "h3_level8_index",
                ]
            ),
            on="notification_number",
            how="inner",
            suffix="_reference",
        )
    )

    comparison = comparison.with_columns(
        (
            pl.col("h3_level8_index")
            ==
            pl.col("h3_level8_index_reference")
        )
        .alias("h3_match")
    )

    total_compared = comparison.height

    matching_records = (
        comparison
        .filter(
            pl.col("h3_match")
        )
        .height
    )

    failed_records = (
        total_compared
        -
        matching_records
    )

    join_error_rate = (
        failed_records / total_compared
        if total_compared
        else 1.0
    )

    JOIN_ERROR_THRESHOLD = 0.05

    metrics.record(
        operation="validate_against_sr_hex",
        start_time=start,
        records=total_compared,
        status=(
            "success"
            if join_error_rate <= JOIN_ERROR_THRESHOLD
            else "failed"
        ),
    )

    # =========================================================
    # 8. Join-error threshold
    # =========================================================

    if join_error_rate > JOIN_ERROR_THRESHOLD:
        raise ValueError(
            "H3 join validation failed. "
            f"Join error rate: {join_error_rate:.2%}. "
            f"Allowed threshold: "
            f"{JOIN_ERROR_THRESHOLD:.2%}."
        )

    # =========================================================
    # 9. Write H3-enriched dataset
    # =========================================================

    start = metrics.start()

    writer.write_dataframe(
        result,
        "H3_output.parquet",
    )

    metrics.record(
        operation="write_h3_output",
        start_time=start,
        records=len(result),
        status="success",
    )

    # =========================================================
    # 10. Write validation summary
    # =========================================================

    validation_result = {
        "pipeline": "city_pipeline",
        "operation": "task1_h3",
        "status": "success",
        "records": len(result),
        "missing_coordinate_records": (
            missing_coordinates
        ),
        "generated_zero_h3_records": (
            zero_h3
        ),
        "reference_records": len(sr_hex),
        "records_compared": total_compared,
        "matching_records": matching_records,
        "failed_records": failed_records,
        "join_error_rate": join_error_rate,
        "join_error_threshold": (
            JOIN_ERROR_THRESHOLD
        ),
    }

    writer.write_json(
        validation_result,
        "task1_h3_validation.json",
    )

    # =========================================================
    # 11. Pipeline metrics
    # =========================================================

    pipeline_result = {
        "pipeline": "city_pipeline",
        "task": "task1_h3",
        "status": "success",
        "total_duration_seconds": round(
            time.perf_counter()
            -
            pipeline_start,
            4,
        ),
        "steps": metrics.get_metrics(),
    }

    writer.write_json(
        pipeline_result,
        "task1_h3_pipeline_metrics.json",
    )

    # =========================================================
    # Final Dagster output
    # =========================================================

    return pipeline_result