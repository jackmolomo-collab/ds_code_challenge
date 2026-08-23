from dagster import op, job

from src.extraction.h3_extractor import H3Extractor
from src.transformations.h3_dataframe import H3DataFrame
from src.validation.h3_validator import H3Validator
from src.validation.schema_validator import SchemaValidator
from src.outputs.result_writer import ResultWriter


SOURCE_KEY = "city-hex-polygons-8.geojson"
SCHEMA_PATH = "/opt/dagster/config/schemas/h3_resolution_8.yaml"
OUTPUT_DIR = "/opt/dagster/output"


@op
def extract_h3():

    extractor = H3Extractor()

    return extractor.extract_resolution_8(
        SOURCE_KEY
    )


@op
def create_h3_dataframe(features):

    transformer = H3DataFrame()

    return transformer.create(
        features
    )


@op
def validate_h3(h3_df):

    validator = H3Validator()

    return validator.validate(
        h3_df
    )


@op
def validate_schema(validated_h3_df):

    validator = SchemaValidator(
        SCHEMA_PATH
    )

    return validator.validate(
        validated_h3_df
    )


@op
def write_h3_dataframe(validated_h3_df):

    writer = ResultWriter(
        OUTPUT_DIR
    )

    return writer.write_dataframe(
        validated_h3_df,
        "h3_resolution_8.json"
    )


@op
def write_schema_result(schema_result):

    writer = ResultWriter(
        OUTPUT_DIR
    )

    return writer.write_json(
        schema_result,
        "schema_validation.json"
    )


@job
def city_pipeline():

    features = extract_h3()

    h3_df = create_h3_dataframe(
        features
    )

    validated_h3_df = validate_h3(
        h3_df
    )

    schema_result = validate_schema(
        validated_h3_df
    )

    write_h3_dataframe(
        validated_h3_df
    )

    write_schema_result(
        schema_result
    )