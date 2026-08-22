from dagster import Definitions, op, job, Config

from src.extraction.s3_client import S3Client
from validation.h3_validator import H3Validator
from outputs.result_writer import ResultWriter


class ChallengeConfig(Config):
    input_file: str = "city-hex-polygons-8-10.geojson"


@op
def extract(config: ChallengeConfig):
    s3 = S3Client()

    geojson = s3.read_geojson(config.input_file)

    return geojson


@op
def validate(geojson):
    validator = H3Validator()

    return validator.validate(geojson)


@op
def write_result(result):
    writer = ResultWriter(
        output_dir="/opt/dagster/output"
    )

    output_file = writer.write_json(
        result,
        "validation_result.json"
    )

    print(f"Output written to: {output_file}")

    return output_file


@job
def city_ds_challenge():
    write_result(
        validate(
            extract()
        )
    )


defs = Definitions(
    jobs=[
        city_ds_challenge
    ]
)