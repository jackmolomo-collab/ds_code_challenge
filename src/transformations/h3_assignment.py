import h3
import polars as pl


class H3Assignment:

    def __init__(self, resolution: int = 8):
        self.resolution = resolution

    def assign(
        self,
        dataframe: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Assign each service request to an H3 cell.

        Missing latitude/longitude values receive
        H3 index '0'.
        """

        required_columns = {
            "latitude",
            "longitude",
        }

        missing_columns = (
            required_columns
            -
            set(dataframe.columns)
        )

        if missing_columns:
            raise ValueError(
                "Missing required columns: "
                f"{sorted(missing_columns)}"
            )

        output_column = (
            f"h3_level{self.resolution}_index"
        )

        return dataframe.with_columns(
            pl.struct(
                [
                    "latitude",
                    "longitude",
                ]
            )
            .map_elements(
                self._calculate_h3,
                return_dtype=pl.String,
            )
            .alias(output_column)
        )

    def _calculate_h3(
        self,
        row: dict,
    ) -> str:

        latitude = row["latitude"]
        longitude = row["longitude"]

        if latitude is None or longitude is None:
            return "0"

        return h3.latlng_to_cell(
            latitude,
            longitude,
            self.resolution,
        )