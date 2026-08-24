from __future__ import annotations

import polars as pl


class AtlantisRequestValidator:
    """
    Validates the Atlantis service-request spatial filter output.

    Validation covers:
    - required columns
    - input/output record counts
    - coordinate validity
    - distance threshold compliance
    - duplicate notification numbers
    """

    REQUIRED_COLUMNS = {
        "notification_number",
        "latitude",
        "longitude",
        "distance_km",
    }

    def __init__(self, distance_threshold_km: float = 1.852):
        if distance_threshold_km <= 0:
            raise ValueError(
                "distance_threshold_km must be greater than 0"
            )

        self.distance_threshold_km = distance_threshold_km

    def validate(
        self,
        input_df: pl.DataFrame,
        filtered_df: pl.DataFrame,
    ) -> dict:
        """
        Validate the filtered Atlantis service requests.

        Returns a structured validation result.
        """

        self._validate_required_columns(filtered_df)

        total_records = input_df.height
        filtered_records = filtered_df.height

        valid_coordinates = input_df.filter(
            pl.col("latitude").is_not_null()
            & pl.col("longitude").is_not_null()
        ).height

        invalid_coordinates = (
            total_records - valid_coordinates
        )

        records_over_threshold = filtered_df.filter(
            pl.col("distance_km")
            > self.distance_threshold_km
        ).height

        duplicate_notification_numbers = (
            filtered_df
            .filter(
                pl.col("notification_number").is_not_null()
            )
            .height
            - filtered_df
            .filter(
                pl.col("notification_number").is_not_null()
            )
            .select(
                pl.col("notification_number")
                .n_unique()
            )
            .item()
        )

        maximum_distance_km = (
            filtered_df
            .select(
                pl.col("distance_km").max()
            )
            .item()
            if filtered_records > 0
            else 0.0
        )

        match_rate = (
            filtered_records / valid_coordinates
            if valid_coordinates > 0
            else 0.0
        )

        distance_validation_passed = (
            records_over_threshold == 0
        )

        duplicate_validation_passed = (
            duplicate_notification_numbers == 0
        )

        required_columns_passed = True

        overall_passed = all(
            [
                required_columns_passed,
                distance_validation_passed,
                duplicate_validation_passed,
            ]
        )

        return {
            "total_records": total_records,
            "valid_coordinate_records": valid_coordinates,
            "invalid_coordinate_records": invalid_coordinates,
            "matched_records": filtered_records,
            "match_rate": round(match_rate, 6),
            "distance_threshold_km": (
                self.distance_threshold_km
            ),
            "records_over_distance_threshold": (
                records_over_threshold
            ),
            "maximum_distance_km": round(
                maximum_distance_km or 0.0,
                6,
            ),
            "duplicate_notification_numbers": (
                duplicate_notification_numbers
            ),
            "required_columns_passed": (
                required_columns_passed
            ),
            "distance_validation_passed": (
                distance_validation_passed
            ),
            "duplicate_validation_passed": (
                duplicate_validation_passed
            ),
            "passed": overall_passed,
        }

    def _validate_required_columns(
        self,
        dataframe: pl.DataFrame,
    ) -> None:
        """
        Ensure the filtered dataset contains the columns
        required for downstream processing.
        """

        missing_columns = (
            self.REQUIRED_COLUMNS
            - set(dataframe.columns)
        )

        if missing_columns:
            raise ValueError(
                "Missing required columns: "
                f"{sorted(missing_columns)}"
            )