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

    nta_field = cols.get("nta2020") or cols.get("ntacode") or cols.get("nta")
    cdta_field = cols.get("cdta2020")
    name_field = cols.get("ntaname") or cols.get("name")

    
    if cdta_field:
        gdf = gdf.rename(columns={cdta_field: "CDTA2020"})
        gdf["CDTA2020"] = gdf["CDTA2020"].astype("string").str.strip().str.upper()
        gdf["NTA_CODE"] = gdf["CDTA2020"]
    elif nta_field:
        gdf = gdf.rename(columns={nta_field: "NTA2020"})
        gdf["NTA2020"] = gdf["NTA2020"].astype("string").str.strip().str.upper()
        gdf["NTA_CODE"] = gdf["NTA2020"].str.slice(0, 4)
    else:
        gdf["NTA2020"] = None
        gdf["NTA_CODE"] = None

    if cdta_field and "CDTA2020" not in gdf.columns:
        gdf = gdf.rename(columns={cdta_field: "CDTA2020"})
        gdf["CDTA2020"] = gdf["CDTA2020"].astype("string").str.strip().str.upper()

    if name_field:
        gdf = gdf.rename(columns={name_field: "NTA_NAME"})
    else:
        gdf["NTA_NAME"] = None

    return gdf[["NTA_CODE", "NTA_NAME", "geometry"]]
