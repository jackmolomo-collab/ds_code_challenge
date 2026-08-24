from __future__ import annotations

import math

import polars as pl


class AtlantisRequestFilter:
    """
    Filters service requests based on their distance
    from an official Atlantis suburb centroid.

    The spatial threshold is expressed in arc minutes.
    One minute of latitude is approximately one nautical mile.
    """

    def __init__(self, distance_minutes: float = 1.0):

        if distance_minutes <= 0:
            raise ValueError(
                "distance_minutes must be greater than 0"
            )

        self.distance_minutes = distance_minutes

    @staticmethod
    def _distance_km(
        latitude: float,
        longitude: float,
        centroid_latitude: float,
        centroid_longitude: float,
    ) -> float:
        """
        Calculate great-circle distance using the Haversine formula.

        Returns distance in kilometres.
        """

        earth_radius_km = 6371.0088

        lat1 = math.radians(latitude)
        lon1 = math.radians(longitude)

        lat2 = math.radians(centroid_latitude)
        lon2 = math.radians(centroid_longitude)

        delta_lat = lat2 - lat1
        delta_lon = lon2 - lon1

        a = (
            math.sin(delta_lat / 2) ** 2
            + math.cos(lat1)
            * math.cos(lat2)
            * math.sin(delta_lon / 2) ** 2
        )

        c = 2 * math.atan2(
            math.sqrt(a),
            math.sqrt(1 - a),
        )

        return earth_radius_km * c

    def filter_requests(
        self,
        service_requests: pl.DataFrame,
        centroid_latitude: float,
        centroid_longitude: float,
    ) -> pl.DataFrame:
        """
        Return service requests within the configured
        distance threshold of the supplied centroid.
        """

        required_columns = {
            "latitude",
            "longitude",
        }

        missing_columns = (
            required_columns
            - set(service_requests.columns)
        )

        if missing_columns:
            raise ValueError(
                "Missing required columns: "
                f"{sorted(missing_columns)}"
            )

        threshold_km = (
            self.distance_minutes
            * 1.852
        )

        valid_coordinates = service_requests.filter(
            pl.col("latitude").is_not_null()
            & pl.col("longitude").is_not_null()
        )

        filtered = valid_coordinates.with_columns(
            pl.struct(
                ["latitude", "longitude"]
            )
            .map_elements(
                lambda row: self._distance_km(
                    row["latitude"],
                    row["longitude"],
                    centroid_latitude,
                    centroid_longitude,
                ),
                return_dtype=pl.Float64,
            )
            .alias("distance_km")
        )

        return filtered.filter(
            pl.col("distance_km") <= threshold_km
        )

    def calculate_metrics(
        self,
        service_requests: pl.DataFrame,
        filtered_requests: pl.DataFrame,
    ) -> dict:
        """
        Calculate filtering metrics for observability
        and validation.
        """

        total_records = service_requests.height

        valid_coordinate_records = (
            service_requests
            .filter(
                pl.col("latitude").is_not_null()
                & pl.col("longitude").is_not_null()
            )
            .height
        )

        matched_records = filtered_requests.height

        unmatched_records = (
            valid_coordinate_records
            - matched_records
        )

        match_rate = (
            matched_records / valid_coordinate_records
            if valid_coordinate_records > 0
            else 0.0
        )

        return {
            "total_records": total_records,
            "valid_coordinate_records": (
                valid_coordinate_records
            ),
            "matched_records": matched_records,
            "unmatched_records": unmatched_records,
            "match_rate": round(match_rate, 6),
            "distance_threshold_minutes": (
                self.distance_minutes
            ),
            "distance_threshold_km": round(
                self.distance_minutes * 1.852,
                6,
            ),
        }