from typing import Tuple
from pyproj import Transformer

# define reusable transformers
to_stateplane = Transformer.from_crs(4326, 2263, always_xy=True)  # lon/lat -> state plane
to_lonlat     = Transformer.from_crs(2263, 4326, always_xy=True)  # state plane -> lon/lat

def get_lonlat_to_stateplane(lon: float, lat: float, epsg: int = 2263) -> Tuple[float, float]:
    """Convert lon/lat (WGS84) to NY State Plane (ft)."""
    return to_stateplane.transform(lon, lat)

def get_stateplane_to_lonlat(x: float, y: float, epsg: int = 2263) -> Tuple[float, float]:
    """Convert state plane (ft) to lon/lat (WGS84)."""
    return to_lonlat.transform(x, y)
