# import json
# import pandas as pd

# from src.extraction.s3_client import S3Client


# class H3DataFrame:

#     def __init__(self):
#         self.s3 = S3Client()

#     def create(self, source):

#         data = json.loads(
#             self.s3.read_geojson(source)
#         )

#         features = data["features"]

#         features_8 = [
#             feature
#             for feature in features
#             if feature["properties"]["resolution"] == 8
#         ]

#         h3_df = pd.DataFrame([
#             {
#                 "h3_index": feature["properties"]["index"],
#                 "centroid_lat": feature["properties"]["centroid_lat"],
#                 "centroid_lon": feature["properties"]["centroid_lon"],
#                 "resolution": feature["properties"]["resolution"],
#                 "geometry": feature["geometry"],
#             }
#             for feature in features_8
#         ])

#         return h3_df



import polars as pd


class H3DataFrame:

    def create(self, features):

        h3_df = pd.DataFrame([
            {
                "h3_index": feature["properties"]["index"],
                "centroid_lat": feature["properties"]["centroid_lat"],
                "centroid_lon": feature["properties"]["centroid_lon"],
                "resolution": feature["properties"]["resolution"],
                "geometry": feature["geometry"],
            }
            for feature in features
        ])

        return h3_df