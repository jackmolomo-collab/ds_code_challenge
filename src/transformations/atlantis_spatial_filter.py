from math import cos, radians

import geopandas as gpd
import polars as pl


class AtlantisSpatialFilter:
    """Filter service requests near official Atlantis-area suburbs."""

    def __init__(self, distance_minutes=1.0):
        self.distance_minutes = distance_minutes

    def identify_atlantis_suburbs(
        self,
        suburbs: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """Identify official suburbs containing Atlantis in their name."""

        suburb_names = (
            suburbs["OFC_SBRB_NAME"]
            .astype(str)
            .str.upper()
        )

        return suburbs[
            suburb_names.str.contains(
                "ATLANTIS",
                na=False,
            )
        ].copy()

    def calculate_centroids(
        self,
        suburbs: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """Calculate geographic centroids for suburb polygons."""

        if suburbs.empty:
            raise ValueError(
                "No Atlantis suburbs were found."
            )

        projected = suburbs.to_crs(
            "EPSG:32734"
        )

        projected["centroid"] = (
            projected.geometry.centroid
        )

        centroids = projected[
            [
                "OFC_SBRB_NAME",
                "centroid",
            ]
        ].copy()

        return gpd.GeoDataFrame(
            centroids,
            geometry="centroid",
            crs="EPSG:32734",
        ).to_crs("EPSG:4326")

    def filter_requests(
        self,
        service_requests: pl.DataFrame,
        centroids: gpd.GeoDataFrame,
    ) -> tuple[pl.DataFrame, dict]:
        """Filter requests within one arc-minute of a suburb centroid."""

        if service_requests.is_empty():
            return service_requests, {
                "input_records": 0,
                "matched_records": 0,
                "unmatched_records": 0,
            }

        request_pdf = service_requests.to_pandas()

        request_gdf = gpd.GeoDataFrame(
            request_pdf,
            geometry=gpd.points_from_xy(
                request_pdf["longitude"],
                request_pdf["latitude"],
            ),
            crs="EPSG:4326",
        )

        # One latitude minute is approximately
        # 1 nautical mile = 1852 metres.
        #
        # Longitude distance varies with latitude.
        # We therefore calculate the equivalent
        # longitude distance at each centroid latitude.

        matches = []

        for _, centroid_row in centroids.iterrows():

            centroid = centroid_row["centroid"]

            latitude = centroid.y

            latitude_distance = (
                self.distance_minutes * 1852
            )

            longitude_distance = (
                latitude_distance
                / cos(radians(latitude))
            )

            centroid_projected = gpd.GeoSeries(
                [centroid],
                crs="EPSG:4326",
            ).to_crs("EPSG:32734").iloc[0]

            request_projected = request_gdf.to_crs(
                "EPSG:32734"
            )

            distances = (
                request_projected.geometry
                .distance(centroid_projected)
            )

            matches.append(
                distances
                <= longitude_distance
            )

        if matches:

            matched_mask = matches[0]

            for mask in matches[1:]:
                matched_mask = (
                    matched_mask | mask
                )

        else:
            matched_mask = []

        filtered = request_gdf[
            matched_mask
        ].drop(
            columns=["geometry"]
        )

        result = pl.from_pandas(
            filtered
        )

        metrics = {
            "input_records": len(service_requests),
            "matched_records": len(result),
            "unmatched_records": (
                len(service_requests)
                - len(result)
            ),
            "match_rate": (
                len(result)
                / len(service_requests)
                if len(service_requests) > 0
                else 0.0
            ),
            "distance_minutes": (
                self.distance_minutes
            ),
            "suburbs_evaluated": len(
                centroids
            ),
        }

        return result, metrics