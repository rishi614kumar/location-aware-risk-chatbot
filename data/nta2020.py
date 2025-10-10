# adapters/data/nta.py  (loader; cached, shared)
from __future__ import annotations
from functools import lru_cache
import geopandas as gpd
from config.settings import NTA_PATH
# NYC Open Data 2020 NTA GeoJSON (WGS84 / EPSG:4326)

@lru_cache(maxsize=1)
def load_nta_2020() -> gpd.GeoDataFrame:
    gdf = gpd.read_file(NTA_PATH)  # loads as EPSG:4326
    # Normalize useful columns
    cols = {c.lower(): c for c in gdf.columns}
    code_col = cols.get("nta2020") or cols.get("nta") or cols.get("ntacode")
    name_col = cols.get("ntaname") or cols.get("name")
    if code_col:
        gdf = gdf.rename(columns={code_col: "NTA_CODE"})
    else:
        gdf["NTA_CODE"] = None
    if name_col:
        gdf = gdf.rename(columns={name_col: "NTA_NAME"})
    else:
        gdf["NTA_NAME"] = None
    return gdf[["NTA_CODE", "NTA_NAME", "geometry"]]
