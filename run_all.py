import sys
import time

from dagster import materialize

from dagster_project.definitions import (
    task1_h3,
    task2_service_requests,
    task5_1_atlantis_requests,
)


def run_asset(asset, task_name):
    """Execute one Dagster asset and fail fast on errors."""

    print()
    print("=" * 70)
    print(task_name)
    print("=" * 70)

    start = time.perf_counter()

    result = materialize([asset])

    duration = time.perf_counter() - start

    if not result.success:
        raise RuntimeError(
            f"{task_name} failed."
        )

    print(
        f"{task_name} completed successfully "
        f"in {duration:.2f} seconds."
    )


def main():
    """Run all completed Data Engineering tasks."""

    pipeline_start = time.perf_counter()

    print()
    print("=" * 70)
    print("CITY OF CAPE TOWN DATA ENGINEERING PIPELINE")
    print("=" * 70)

    try:

        # --------------------------------------------------------
        # TASK 1
        # --------------------------------------------------------

        run_asset(
            task1_h3,
            "TASK 1 - H3 RESOLUTION 8",
        )

        # --------------------------------------------------------
        # TASK 2
        # --------------------------------------------------------

        run_asset(
            task2_service_requests,
            "TASK 2 - SERVICE REQUEST H3 ASSIGNMENT",
        )

        # --------------------------------------------------------
        # TASK 5.1
        # --------------------------------------------------------

        run_asset(
            task5_1_atlantis_requests,
            "TASK 5.1 - ATLANTIS SERVICE REQUEST FILTER",
        )

        # --------------------------------------------------------
        # COMPLETE
        # --------------------------------------------------------

        total_duration = (
            time.perf_counter()
            - pipeline_start
        )

        print()
        print("=" * 70)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print(
            f"Total runtime: "
            f"{total_duration:.2f} seconds"
        )

        print()
        print("All task outputs are available in:")
        print("/opt/dagster/output")

        print("=" * 70)

    except Exception as error:

        total_duration = (
            time.perf_counter()
            - pipeline_start
        )

        print()
        print("=" * 70)
        print("PIPELINE FAILED")
        print("=" * 70)

        print(
            f"Runtime before failure: "
            f"{total_duration:.2f} seconds"
        )

        print(
            f"Error: {error}"
        )

        print("=" * 70)

        sys.exit(1)


if __name__ == "__main__":
    main()