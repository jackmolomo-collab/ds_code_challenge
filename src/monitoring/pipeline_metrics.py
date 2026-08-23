import json
import time
from pathlib import Path


class PipelineMetrics:

    def __init__(self, output_dir):
        self.metrics = []
        self.output_dir = Path(output_dir)

    def start(self):
        return time.perf_counter()

    def record(
        self,
        operation,
        start_time,
        records=None,
        status="success"
    ):

        duration = time.perf_counter() - start_time

        metric = {
            "operation": operation,
            "status": status,
            "duration_seconds": round(duration, 4)
        }

        if records is not None:
            metric["records"] = records

        self.metrics.append(metric)

        return metric

    def total_duration(self, start_time):
        return round(
            time.perf_counter() - start_time,
            4
        )

    def get_metrics(self):
        return self.metrics

    def write(self, filename="pipeline_metrics.json"):

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        output_path = self.output_dir / filename

        with open(output_path, "w") as f:
            json.dump(
                self.metrics,
                f,
                indent=2
            )

        return output_path