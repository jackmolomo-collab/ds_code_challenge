from dagster import op, job

from validation.h3_validator import H3Validator
from outputs.result_writer import ResultWriter


@op
def extract():

    return "city-hex-polygons-8-10.geojson"


@op
def validate(source):

    validator = H3Validator()

    return validator.validate(source)


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

    source = extract()

    result = validate(source)

    write_result(result)