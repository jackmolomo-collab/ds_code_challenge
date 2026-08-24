import polars as pd


class H3AssignmentValidator:

    def __init__(
        self,
        error_threshold: float = 0.05,
    ):
        self.error_threshold = error_threshold

    def validate(
        self,
        assigned_dataframe: pd.DataFrame,
        reference_dataframe: pd.DataFrame,
    ) -> dict:
        """
        Validate generated H3 assignments against
        the reference sr_hex dataset.

        The reference dataset contains the expected
        h3_level8_index for each service request.
        """

        required_columns = {
            "notification_number",
            "h3_level8_index",
        }

        for dataframe_name, dataframe in [
            ("assigned_dataframe", assigned_dataframe),
            ("reference_dataframe", reference_dataframe),
        ]:

            missing_columns = (
                required_columns
                - set(dataframe.columns)
            )

            if missing_columns:

                raise ValueError(
                    f"{dataframe_name} is missing "
                    f"required columns: "
                    f"{sorted(missing_columns)}"
                )

        assigned = assigned_dataframe.select(
            [
                "notification_number",
                "h3_level8_index",
            ]
        )

        reference = reference_dataframe.select(
            [
                "notification_number",
                "h3_level8_index",
            ]
        )

        comparison = assigned.join(
            reference,
            on="notification_number",
            how="inner",
            suffix="_reference",
        )

        if comparison.height == 0:

            raise ValueError(
                "No records could be matched between "
                "the generated dataset and the "
                "reference dataset."
            )

        comparison = comparison.with_columns(
            (
                pd.col("h3_level8_index")
                ==
                pd.col("h3_level8_index_reference")
            )
            .alias("h3_match")
        )

        total_records = comparison.height

        matched_records = comparison.filter(
            pd.col("h3_match")
        ).height

        mismatched_records = (
            total_records - matched_records
        )

        error_rate = (
            mismatched_records / total_records
            if total_records
            else 1.0
        )

        match_rate = (
            matched_records / total_records
            if total_records
            else 0.0
        )

        passed = (
            error_rate <= self.error_threshold
        )

        print(
            f"Reference records matched: "
            f"{total_records}"
        )

        print(
            f"H3 assignments matched: "
            f"{matched_records}"
        )

        print(
            f"H3 assignments mismatched: "
            f"{mismatched_records}"
        )

        print(
            f"H3 match rate: "
            f"{match_rate:.4%}"
        )

        print(
            f"H3 error rate: "
            f"{error_rate:.4%}"
        )

        print(
            f"H3 error threshold: "
            f"{self.error_threshold:.2%}"
        )

        print(
            f"H3 assignment validation passed: "
            f"{passed}"
        )

        if not passed:

            raise ValueError(
                "H3 assignment validation failed. "
                f"Error rate {error_rate:.4%} "
                f"exceeds threshold "
                f"{self.error_threshold:.2%}."
            )

        return {
            "total_records": total_records,
            "matched_records": matched_records,
            "mismatched_records": mismatched_records,
            "match_rate": match_rate,
            "error_rate": error_rate,
            "error_threshold": self.error_threshold,
            "passed": passed,
        }