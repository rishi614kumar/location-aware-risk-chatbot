from typing import Optional

def get_bbl_from_lonlat(lon: float, lat: float) -> Optional[str]:
    """Point-in-polygon on PLUTO to get containing BBL."""
    return None

def get_bbls_near_lonlat(lon: float, lat: float, buffer_ft: float = 25.0) -> list[str]:
    """Return nearby BBLs within buffer (units?)."""
    return []

def get_lonlat_from_bbl(bbl: str) -> Optional[tuple[float, float]]:
    """Centroid or representative point for a BBL."""
    return None