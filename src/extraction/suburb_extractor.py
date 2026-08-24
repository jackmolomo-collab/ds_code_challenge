import io

import geopandas as gpd
import requests


OFFICIAL_SUBURB_URL = (
    "https://citymaps.capetown.gov.za/"
    "agsext/rest/services/"
    "Theme_Based/Political_Administrative_Boundaries/"
    "MapServer/4/query"
)


class SuburbExtractor:
    """Extract official City of Cape Town suburb boundaries."""

    def __init__(self, url=OFFICIAL_SUBURB_URL):
        self.url = url

    def extract(self):
        """Download official suburb polygons as a GeoDataFrame."""

        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
        }

        response = requests.get(
            self.url,
            params=params,
            timeout=60,
        )

        response.raise_for_status()

        return gpd.read_file(
            io.BytesIO(response.content)
        )