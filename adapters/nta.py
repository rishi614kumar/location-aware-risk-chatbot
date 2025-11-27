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
    try:
        info = _get_geoclient().bbl(str(bbl))
    except GeoclientException as exc:
        logger.warning(f"Geoclient NTA lookup failed for BBL {bbl}: {exc}")
        return None
    except Exception as exc:
        logger.error(f"Unexpected Geoclient error for BBL {bbl}: {exc}")
        return None

    nta = info.get("nta")
    if not nta:
        return None
    code = str(nta).strip().upper()
    return code or None


def _nta_from_spatial(bbl: str) -> Optional[str]:
    pluto = load_pluto_geom()
    lot = pluto.loc[pluto["BBL"] == str(bbl)]
    if lot.empty:
        return None

    nta = load_nta_2020().to_crs(pluto.crs)
    joined = gpd.sjoin(lot[["geometry"]], nta, how="left", predicate="intersects")
    if joined.empty:
        return None
    val = joined.iloc[0].get("NTA_CODE")
    return str(val).strip().upper() if val else None


@lru_cache(maxsize=2048)
def get_nta_from_bbl(bbl: str) -> Optional[str]:
    """
    Prefer Geoclient for BBL → NTA lookups to match city APIs.
    Falls back to the spatial join with local NTA polygons if needed.
    Returns NTA code (e.g., 'MN08') or None.
    """
    if not bbl:
        return None

    code = _nta_from_geoclient(str(bbl))
    if code:
        return code

    logger.debug(f"Falling back to spatial NTA lookup for BBL {bbl}")
    return _nta_from_spatial(str(bbl))

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