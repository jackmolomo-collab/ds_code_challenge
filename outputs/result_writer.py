import json
from pathlib import Path


class ResultWriter:

    def __init__(self, output_dir="output"):
        self.output_dir = Path(output_dir)

    def write_json(self, result, filename):

        output_file = self.output_dir / filename

        with open(output_file, "w") as file:
            json.dump(
                result,
                file,
                indent=4
            )

        return output_file