from dagster import op, job
import time

from src.extraction.h3_extractor import H3Extractor
from src.transformations.h3_dataframe import H3DataFrame
from src.validation.h3_validator import H3Validator
from src.validation.schema_validator import SchemaValidator
from src.outputs.result_writer import ResultWriter
from src.monitoring.pipeline_metrics import PipelineMetrics


SOURCE_KEY = "city-hex-polygons-8.geojson"
SCHEMA_PATH = "/opt/dagster/config/schemas/h3_resolution_8.yaml"
OUTPUT_DIR = "/opt/dagster/output"


# ============================================================
# EXTRACT
# ============================================================

@op
def extract_h3():

    start = time.perf_counter()

    try:

        extractor = H3Extractor()

        features = extractor.extract_resolution_8(
            SOURCE_KEY
        )

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "extract_h3",
            "status": "success",
            "duration_seconds": duration,
            "records": len(features)
        }

        return {
            "data": features,
            "metric": metric
        }

    except Exception:

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "extract_h3",
            "status": "failed",
            "duration_seconds": duration
        }

        raise


# ============================================================
# TRANSFORMATION
# ============================================================

@op
def create_h3_dataframe(extract_result):

    start = time.perf_counter()

    try:

        transformer = H3DataFrame()

        h3_df = transformer.create(
            extract_result["data"]
        )

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "create_h3_dataframe",
            "status": "success",
            "duration_seconds": duration,
            "records": len(h3_df)
        }

        return {
            "data": h3_df,
            "metrics": [
                extract_result["metric"],
                metric
            ]
        }

    except Exception:

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "create_h3_dataframe",
            "status": "failed",
            "duration_seconds": duration
        }

        raise


# ============================================================
# H3 VALIDATION
# ============================================================

@op
def validate_h3(data_result):

    start = time.perf_counter()

    try:

        validator = H3Validator()

        validated_h3_df = validator.validate(
            data_result["data"]
        )

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "validate_h3",
            "status": "success",
            "duration_seconds": duration,
            "records": len(validated_h3_df)
        }

        return {
            "data": validated_h3_df,
            "metrics": data_result["metrics"] + [metric]
        }

    except Exception:

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "validate_h3",
            "status": "failed",
            "duration_seconds": duration
        }

        raise


# ============================================================
# SCHEMA VALIDATION
# ============================================================

@op
def validate_schema(validation_result):

    start = time.perf_counter()

    try:

        validator = SchemaValidator(
            SCHEMA_PATH
        )

        schema_result = validator.validate(
            validation_result["data"]
        )

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "validate_schema",
            "status": "success",
            "duration_seconds": duration
        }

        return {
            "data": schema_result,
            "metrics": validation_result["metrics"] + [metric]
        }

    except Exception:

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "validate_schema",
            "status": "failed",
            "duration_seconds": duration
        }

        raise


# ============================================================
# WRITE H3 DATAFRAME
# ============================================================

@op
def write_h3_dataframe(validation_result):

    start = time.perf_counter()

    try:

        writer = ResultWriter(
            OUTPUT_DIR
        )

        output_path = writer.write_dataframe(
            validation_result["data"],
            "h3_resolution_8.json"
        )

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "write_h3_dataframe",
            "status": "success",
            "duration_seconds": duration
        }

        return {
            "output_path": output_path,
            "metrics": validation_result["metrics"] + [metric]
        }

    except Exception:

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "write_h3_dataframe",
            "status": "failed",
            "duration_seconds": duration
        }

        raise


# ============================================================
# WRITE SCHEMA RESULT
# ============================================================

@op
def write_schema_result(
    schema_result,
    validation_result
):

    start = time.perf_counter()

    try:

        writer = ResultWriter(
            OUTPUT_DIR
        )

        output_path = writer.write_json(
            schema_result["data"],
            "schema_validation.json"
        )

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "write_schema_result",
            "status": "success",
            "duration_seconds": duration
        }

        return {
            "output_path": output_path,
            "metrics": validation_result["metrics"] + [metric]
        }

    except Exception:

        duration = round(
            time.perf_counter() - start,
            4
        )

        metric = {
            "operation": "write_schema_result",
            "status": "failed",
            "duration_seconds": duration
        }

        raise


# ============================================================
# PIPELINE MONITORING
# ============================================================

@op
def write_pipeline_metrics(
    h3_write_result,
    schema_write_result
):

    pipeline_start = time.perf_counter()

    metrics = PipelineMetrics(
        OUTPUT_DIR
    )

 

    pipeline_metrics = list(
        schema_write_result["metrics"]
    )

    h3_write_metric = h3_write_result["metrics"][-1]

    pipeline_metrics.append(
        h3_write_metric
    )

    metrics.metrics = pipeline_metrics

    total_duration = round(
        time.perf_counter() - pipeline_start,
        4
    )

    monitoring_result = {
        "pipeline": "city_pipeline",
        "status": "success",
        "total_duration_seconds": total_duration,
        "steps": pipeline_metrics
    }

    import json
    from pathlib import Path

    output_path = Path(
        OUTPUT_DIR
    ) / "pipeline_metrics.json"

    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            monitoring_result,
            file,
            indent=2
        )

    return str(output_path)


# ============================================================
# DAGSTER JOB
# ============================================================

@job
def city_pipeline():

    extract_result = extract_h3()

    dataframe_result = create_h3_dataframe(
        extract_result
    )

    validation_result = validate_h3(
        dataframe_result
    )

    schema_result = validate_schema(
        validation_result
    )

    h3_write_result = write_h3_dataframe(
        validation_result
    )

    schema_write_result = write_schema_result(
        schema_result,
        validation_result
    )

    write_pipeline_metrics(
        h3_write_result,
        schema_write_result
    )