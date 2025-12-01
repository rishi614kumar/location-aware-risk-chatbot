from __future__ import annotations
from typing import Optional, List
from functools import lru_cache
import geopandas as gpd
from api.GeoClient import Geoclient, GeoclientException
from config.logger import logger
from data.pluto import load_pluto_geom
from data.nta2020 import load_nta_2020

_geoclient: Optional[Geoclient] = None

def _get_geoclient() -> Geoclient:
    global _geoclient
    if _geoclient is None:
        _geoclient = Geoclient()
    return _geoclient

def _nta_from_geoclient(bbl: str) -> Optional[str]:
    """
    Retrieves NTA code from Geoclient API.
    Prioritizes the 2010 'nta' code (e.g., 'MN24') to match legacy datasets.
    """
    try:
        # Ensure BBL is a string for the API
        info = _get_geoclient().bbl(str(bbl))
        
        # DEBUG: Log what keys we actually got to debug mismatches
        # logger.debug(f"Geoclient keys for {bbl}: {list(info.keys())}")
    except GeoclientException as exc:
        logger.warning(f"Geoclient NTA lookup failed for BBL {bbl}: {exc}")
        return None
    except Exception as exc:
        logger.error(f"Unexpected Geoclient error for BBL {bbl}: {exc}")
        return None

    # 1. Try standard 'nta' (This is usually the 2010 code: e.g. 'MN24')
    # This is what matches most NYC Open Data 'Population' datasets.
    nta = info.get("nta")
    
    # 2. Fallback: If 'nta' is empty, try 'nta2020' if you need modern codes
    if not nta:
        nta = info.get("nta2020")
        if nta:
             logger.info(f"Using NTA 2020 code for BBL {bbl}: {nta}")

    if not nta:
        return None

    code = str(nta).strip().upper()
    return code or None

def _nta_from_spatial(bbl: str) -> Optional[str]:
    """
    Fallback method using spatial join.
    WARNING: This uses 2020 Boundaries. It will return 2020 codes (e.g., MN0201).
    This may NOT match datasets using 2010 codes (MN24).
    """
    pluto = load_pluto_geom()
    lot = pluto.loc[pluto["BBL"] == str(bbl)]
    if lot.empty:
        return None

    # Note: This loads 2020 shapes.
    nta_gdf = load_nta_2020().to_crs(pluto.crs)
    
    joined = gpd.sjoin(lot[["geometry"]], nta_gdf, how="left", predicate="intersects")
    if joined.empty:
        return None
    
    # Usually the column is 'NTA2020' or 'NTA_CODE' depending on your specific file
    # We try both to be safe
    val = joined.iloc[0].get("NTA2020") or joined.iloc[0].get("NTA_CODE")
    
    return str(val).strip().upper() if val else None

@lru_cache(maxsize=2048)
def get_nta_from_bbl(bbl: str) -> Optional[str]:
    """
    Prefer Geoclient for BBL → NTA lookups to match city APIs.
    Falls back to the spatial join with local NTA polygons if needed.
    Returns NTA code (e.g., 'MN24') or None.
    """
    if not bbl:
        return None

    # Step 1: Try Gold Source (Geoclient)
    code = _nta_from_geoclient(str(bbl))
    if code:
        # logger.info(f"Geoclient found NTA {code} for BBL {bbl}")
        return code

    # Step 2: Fallback
    logger.warning(f"Falling back to spatial NTA lookup for BBL {bbl}. Warning: This may yield NTA2020 codes which differ from NTA2010.")
    return _nta_from_spatial(str(bbl))

def get_bbls_from_nta(nta_code: str) -> List[str]:
    """
    Return all BBLs whose polygons intersect the given NTA code.
    Uses spatial join (NTA polygon -> PLUTO lots).
    """
    pluto = load_pluto_geom()
    nta = load_nta_2020().to_crs(pluto.crs)
    
    # Check both standard columns for 2020 matches
    if "NTA2020" in nta.columns:
        col_name = "NTA2020"
    else:
        col_name = "NTA_CODE"

    sel = nta[nta[col_name].str.upper() == nta_code.upper()]
    if sel.empty:
        # Fallback: Maybe the user passed a 2010 code (MN24) but we only have 2020 map?
        # In that case, we can't map it spatially without the 2010 shapefile.
        logger.warning(f"Could not find NTA polygon for code {nta_code} in 2020 dataset.")
        return []

    hit = gpd.sjoin(pluto[["BBL", "geometry"]], sel[["geometry"]], how="inner", predicate="intersects")
    return sorted({str(b) for b in hit["BBL"].unique()})