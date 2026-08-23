from pathlib import Path
import time

from dagster import asset

from src.extraction.h3_extractor import H3Extractor
from src.transformations.h3_dataframe import H3DataFrame
from src.validation.h3_validator import H3Validator
from src.validation.schema_validator import SchemaValidator
from src.outputs.result_writer import ResultWriter
from src.monitoring.pipeline_metrics import PipelineMetrics


SOURCE_KEY = "city-hex-polygons-8-10.geojson"
SCHEMA_PATH = Path(
    "/opt/dagster/config/schemas/h3_resolution_8.yaml"
)

OUTPUT_DIR = Path("/opt/dagster/output")

H3_OUTPUT_FILE = "H3_output.parquet"
SCHEMA_OUTPUT_FILE = "task1_schema_validation.json"
VALIDATION_OUTPUT_FILE = "task1_h3_validation.json"
METRICS_OUTPUT_FILE = "task1_h3_pipeline_metrics.json"


@asset
def task1_h3():

    pipeline_start = time.perf_counter()

    # ---------------------------------------------------------
    # Confi
    # ---------------------------------------------------------

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    writer = ResultWriter(
        output_dir=OUTPUT_DIR
    )

    metrics = PipelineMetrics(
        output_dir=OUTPUT_DIR
    )

    # ---------------------------------------------------------
    # 1. Extract H3 resolution 8 data
    # ---------------------------------------------------------

    start = metrics.start()

    extractor = H3Extractor()

    features = extractor.extract_resolution_8(
        SOURCE_KEY
    )

    metrics.record(
        operation="extract_h3_resolution_8",
        start_time=start,
        records=len(features),
        status="success",
    )

    # ---------------------------------------------------------
    # 2. Create H3 dataframe
    # ---------------------------------------------------------

    start = metrics.start()

    transformer = H3DataFrame()

    h3_df = transformer.create(
        features
    )

    metrics.record(
        operation="create_h3_dataframe",
        start_time=start,
        records=len(h3_df),
        status="success",
    )

    # ---------------------------------------------------------
    # 3. H3 validation
    # ---------------------------------------------------------

    start = metrics.start()

    h3_validator = H3Validator()

    validated_h3_df = h3_validator.validate(
        h3_df
    )

    metrics.record(
        operation="validate_h3",
        start_time=start,
        records=len(validated_h3_df),
        status="success",
    )

    # ---------------------------------------------------------
    # 4. Schema validation
    # ---------------------------------------------------------

    start = metrics.start()

    schema_validator = SchemaValidator(
        schema_path=SCHEMA_PATH
    )

    schema_result = schema_validator.validate(
        validated_h3_df
    )

    # IMPORTANT:
    # This file must appear on Windows as:
    #
    # C:\Projects\City of Cape Town\
    # Project\ds_code_challenge\output\
    # task1_schema_validation.json

    writer.write_json(
        schema_result,
        SCHEMA_OUTPUT_FILE,
    )

    metrics.record(
        operation="validate_h3_schema",
        start_time=start,
        records=len(validated_h3_df),
        status=(
            "success"
            if schema_result["passed"]
            else "failed"
        ),
    )

    # ---------------------------------------------------------
    # 5. Stop if schema validation fails
    # ---------------------------------------------------------

    if not schema_result["passed"]:
        raise ValueError(
            "H3 resolution 8 schema validation failed. "
            f"See {SCHEMA_OUTPUT_FILE}."
        )

    # ---------------------------------------------------------
    # 6. Write H3 output
    # ---------------------------------------------------------

    start = metrics.start()

    output_path = writer.write_dataframe(
        validated_h3_df,
        H3_OUTPUT_FILE,
    )

    metrics.record(
        operation="write_h3_output",
        start_time=start,
        records=len(validated_h3_df),
        status="success",
    )

    # ---------------------------------------------------------
    # 7. Validation summary
    # ---------------------------------------------------------

    validation_result = {
        "pipeline": "city_pipeline",
        "task": "task1_h3",
        "status": "success",
        "records": len(validated_h3_df),
        "schema_score": schema_result.get(
            "score"
        ),
        "schema_passed_checks": schema_result.get(
            "passed_checks"
        ),
        "schema_total_checks": schema_result.get(
            "total_checks"
        ),
        "h3_output": H3_OUTPUT_FILE,
        "schema_validation_output": (
            SCHEMA_OUTPUT_FILE
        ),
    }

    writer.write_json(
        validation_result,
        VALIDATION_OUTPUT_FILE,
    )

    # ---------------------------------------------------------
    # 8. Pipeline metrics
    # ---------------------------------------------------------

    pipeline_result = {
        "pipeline": "city_pipeline",
        "task": "task1_h3",
        "status": "success",
        "total_duration_seconds": round(
            time.perf_counter()
            - pipeline_start,
            4,
        ),
        "output_directory": str(
            OUTPUT_DIR
        ),
        "outputs": {
            "h3": H3_OUTPUT_FILE,
            "schema_validation": (
                SCHEMA_OUTPUT_FILE
            ),
            "validation": (
                VALIDATION_OUTPUT_FILE
            ),
            "metrics": (
                METRICS_OUTPUT_FILE
            ),
        },
        "steps": metrics.get_metrics(),
    }

    writer.write_json(
        pipeline_result,
        METRICS_OUTPUT_FILE,
    )

    return pipeline_result