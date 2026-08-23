import time


class PipelineMetrics:

    def __init__(self):
        self.metrics = []

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
    