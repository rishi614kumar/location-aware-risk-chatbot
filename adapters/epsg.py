from typing import Tuple

def get_lonlat_to_stateplane(lon: float, lat: float, epsg: int = 2263) -> Tuple[float, float]:
    """Convert lon/lat (WGS84) to NY State Plane (ft)."""
    return (0.0, 0.0)

def get_stateplane_to_lonlat(x: float, y: float, epsg: int = 2263) -> Tuple[float, float]:
    """Convert state plane (ft) to lon/lat (WGS84)."""
    return (0.0, 0.0)
