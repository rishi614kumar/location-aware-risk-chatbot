from __future__ import annotations
from typing import Optional, List
import geopandas as gpd
from data.pluto import load_pluto_geom
from config.settings import NTA_PATH
from data.nta2020 import load_nta_2020

def get_nta_from_bbl(bbl: str) -> Optional[str]:
    """
    Do a spatial join with the 2020 NTA polygons (robust).
    Returns NTA code (e.g., 'MN0604') or None.
    """
    # spatial join with NTA polygons
    pluto = load_pluto_geom()
    lot = pluto.loc[pluto["BBL"] == str(bbl)]
    if lot.empty:
        return None

    nta = load_nta_2020().to_crs(pluto.crs)  # match to EPSG:2263
    joined = gpd.sjoin(lot[["geometry"]], nta, how="left", predicate="intersects")
    if joined.empty:
        return None
    return joined.iloc[0]["NTA_CODE"]  # or "NTA_NAME"

def get_bbls_from_nta(nta_code: str) -> List[str]:
    """
    Return all BBLs whose polygons intersect the given NTA code.
    Uses spatial join (NTA polygon -> PLUTO lots).
    """
    pluto = load_pluto_geom()
    nta = load_nta_2020().to_crs(pluto.crs)
    sel = nta[nta["NTA_CODE"].str.upper() == nta_code.upper()]
    if sel.empty:
        return []

    hit = gpd.sjoin(pluto[["BBL", "geometry"]], sel[["geometry"]], how="inner", predicate="intersects")
    return sorted({str(b) for b in hit["BBL"].unique()})