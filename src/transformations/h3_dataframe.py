import polars as pl


class H3DataFrame:

    def create(self, features):

        h3_df = pl.DataFrame([
            {
                "h3_index": feature["properties"]["index"],
                "centroid_lat": feature["properties"]["centroid_lat"],
                "centroid_lon": feature["properties"]["centroid_lon"],
                "resolution": 8,
                "geometry": feature["geometry"],
            }
            for feature in features
        ])

        return h3_df