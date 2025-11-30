from __future__ import annotations
import threading
from functools import lru_cache
from typing import Dict, Any, List

# Reuse your existing modules/utilities
from api.GeoClient import Geoclient, get_bbl_from_address
from adapters.precinct import get_precinct_from_bbl
from adapters.schemas import GeoBundle
from config.logger import logger


_gc = Geoclient()
_CACHE_LOCK = threading.RLock()

def _standardize_bundle(base: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize keys and leave room for future fields.
    Everything optional and easy to extend.
    """
    base = base or {}
    bundle = {
        # echo input
        "input": base.get("input"),             # e.g., {"type":"address", "address":"...", "borough":"..."} or {"type":"bbl","bbl":"..."}
        # primary ids
        "bbl": base.get("bbl"),
        "bins": base.get("bins", []),           # List[str]
        # location context
        "borough": base.get("borough"),
        "nta": base.get("nta"),
        "communityDistrict": base.get("cd"),
        "precinct": base.get("precinct"),
        # coordinates (if known)
        "latitude": base.get("latitude"),
        "longitude": base.get("longitude"),
        # diagnostics / provenance
        "sources": base.get("sources", {}),     # e.g., {"precinct":"geoclient"} or {"precinct":"pluto"}
        "grc": base.get("grc"),                 # geosupport return code (00 = exact)
        # placeholder for easy future extension
        "extras": base.get("extras", {}),       # e.g., zoning, council, flood, etc.
    }
    return GeoBundle(**bundle)


def _extract_bins(raw: Dict[str, Any]) -> List[str]:
    bins = set()

    if not isinstance(raw, dict):
        return []

    def _add(value: Any) -> None:
        if not value:
            return
        value_str = str(value).strip()
        if value_str.isdigit():
            bins.add(value_str)

    _add(raw.get("buildingIdentificationNumber"))
    _add(raw.get("bin"))

    for key, value in raw.items():
        if key and key.lower().startswith("gibuildingidentificationnumber"):
            _add(value)

    return sorted(bins)

def geo_from_address(address: str, borough: str) -> Dict[str, Any]:
    """
    Address -> Geoclient for full context (precinct/NTA/coords),
    BBL -> (from Geoclient), BINs -> (from Geoclient via BBL),
    Precinct priority = Geoclient (not PLUTO).
    """
    # Geoclient (single address call)
    info = _gc.address_string(address, borough)

    # Primary ids
    bbl = info.get("bbl")
    raw = info.get("raw", {})
    bins: List[str] = _extract_bins(raw) if bbl else []

    # Prefer precinct/nta/coords directly from Geoclient for address flow
    bundle = {
        "input": {"type": "address", "address": address, "borough": borough},
        "bbl": bbl,
        "bins": bins,
        "borough": info.get("borough"),
        "nta": info.get("nta"),
        "cd": info.get("communityDistrict"),
        "precinct": info.get("policePrecinct"),        # <- Geoclient source of truth for address flow
        "latitude": info.get("latitude"),
        "longitude": info.get("longitude"),
        "grc": info.get("grc"),
        "sources": {"precinct": "geoclient"},
    }
    return _standardize_bundle(bundle)


@lru_cache(maxsize=4096)
def _geo_from_bbl_cached(bbl: str) -> GeoBundle:
    """
    BBL -> precinct from PLUTO (fast local),
    Augment with Geoclient fields if available (borough/nta may be present).
    """
    info = _gc.bbl(bbl)
    raw = info.get("raw", {})
    bins = _extract_bins(raw)

    precinct = info.get("policePrecinct")
    if precinct is None:
        try:
            precinct = get_precinct_from_bbl(bbl)
            precinct_source = "pluto"
        except Exception as exc:
            logger.warning(f"Precinct fallback failed for BBL %s: %s", bbl, exc)
            precinct_source = "unknown"
    else:
        precinct_source = "geoclient"

    bundle = {
        "input": {"type": "bbl", "bbl": bbl},
        "bbl": bbl,
        "bins": bins,
        "borough": info.get("borough"),
        "nta": info.get("nta"),
        "cd": info.get("communityDistrict"),
        "precinct": str(precinct) if precinct is not None else None,
        "latitude": info.get("latitude"),
        "longitude": info.get("longitude"),
        "grc": info.get("grc"),
        "sources": {"precinct": precinct_source},
    }
    return GeoBundle(**bundle)


def geo_from_bbl(bbl: str) -> Dict[str, Any]:
    with _CACHE_LOCK:
        bundle = _geo_from_bbl_cached(str(bbl))
    return bundle.copy(deep=True)


def geo_from_bbl_cache_info():
    return _geo_from_bbl_cached.cache_info()

