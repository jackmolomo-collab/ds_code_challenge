import os
import json
import pandas as pd


class ResultWriter:

    def __init__(self, output_dir):
        self.output_dir = output_dir

        os.makedirs(
            self.output_dir,
            exist_ok=True
        )

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

        dataframe.to_json(
            output_file,
            orient="records",
            indent=4
        )

        return output_file