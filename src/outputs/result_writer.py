import os
import json


class ResultWriter:

    def __init__(self, output_dir):
        self.output_dir = output_dir

    def write_json(self, data, filename):

        output_file = os.path.join(
            self.output_dir,
            filename
        )

        with open(
            output_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                data,
                f,
                indent=4,
                default=str
            )

        return output_file

    def write_dataframe(self, dataframe, filename):

        output_file = os.path.join(
            self.output_dir,
            filename
        )

        records = dataframe.to_dicts()

        with open(
            output_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                records,
                f,
                indent=4,
                default=str
            )

        print(f"Output written: {output_file}")
        print(f"Records written: {len(records)}")

        return output_file