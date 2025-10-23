from __future__ import annotations
from typing import Optional, Dict, Any, List

# Reuse your existing modules/utilities
from api.geoclient import Geoclient, get_bbl_from_address, get_bins_from_bbl
from adapters.precinct import get_precinct_from_bbl
from adapters.schemas import GeoBundle, SourceMeta


_gc = Geoclient()

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
    bins: List[str] = get_bins_from_bbl(bbl) if bbl else []

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

def geo_from_bbl(bbl: str) -> Dict[str, Any]:
    """
    BBL -> precinct from PLUTO (fast local),
    Augment with Geoclient fields if available (borough/nta may be present).
    """
    # PLUTO for precinct
    precinct = get_precinct_from_bbl(bbl)

    # Geoclient to enrich (NTA, maybe borough; precinct here is from PLUTO by design)
    info = _gc.bbl(bbl)
    bins: List[str] = get_bins_from_bbl(bbl)  # often works for lots with structures

    bundle = {
        "input": {"type": "bbl", "bbl": bbl},
        "bbl": bbl,
        "bins": bins,
        "borough": info.get("borough"),
        "nta": info.get("nta"),
        "cd": info.get("communityDistrict"),
        "precinct": str(precinct) if precinct is not None else None,  # <- PLUTO source of truth for BBL flow
        "latitude": info.get("latitude"),
        "longitude": info.get("longitude"),
        "grc": info.get("grc"),
        "sources": {"precinct": "pluto"},
    }
    return _standardize_bundle(bundle)

