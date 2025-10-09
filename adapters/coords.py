from __future__ import annotations
import geopandas as gpd
import pandas as pd
from typing import Optional
from shapely.geometry import Point
from functools import lru_cache
from adapters.epsg import get_lonlat_to_stateplane, get_stateplane_to_lonlat
from data.pluto import load_pluto_geom 

def _pt2263(lon: float, lat: float) -> Point:
    x, y = get_lonlat_to_stateplane(lon, lat)
    return Point(x, y)

def get_bbl_from_lonlat(lon: float, lat: float) -> Optional[tuple[str, float]]:
    """Return (BBL, distance_ft) of the nearest MapPLUTO lot to the given lon/lat"""
    # polygons in EPSG:2263 (feet)
    pluto = load_pluto_geom()
    # shapely Point in EPSG:2263       
    pt = _pt2263(lon, lat)

    # One-point GeoDataFrame for the join
    gpt = gpd.GeoDataFrame({"_id":[0]}, geometry=[pt], crs=pluto.crs)

    # Get nearest point and distance
    hit = gpd.sjoin_nearest(
        gpt,
        pluto[["BBL","geometry"]],
        how="left",
        distance_col="_dist_ft",
    )

    if hit.empty or pd.isna(hit.iloc[0]["BBL"]):
        return None

    return str(hit.iloc[0]["BBL"]), float(hit.iloc[0]["_dist_ft"])

def get_bbls_near_lonlat(lon: float, lat: float, buffer_ft: float = 25.0) -> list[str]:
    """Return nearby BBLs within buffer (feet)"""

    pluto = load_pluto_geom()

    # Add buffer to point
    pt = _pt2263(lon, lat).buffer(float(buffer_ft))

    # Check if any geometries intersect the buffer
    idxs = list(pluto.sindex.query(pt, predicate="intersects"))
    if not idxs:
        return []
    near = pluto.iloc[idxs]
    near = near[near.intersects(pt)]
    return sorted({str(b) for b in near["BBL"]})

def get_lonlat_from_bbl(bbl: str) -> Optional[tuple[float, float]]:
    """Centroid or representative point for a BBL"""

    pluto = load_pluto_geom()
    rows = pluto[pluto["BBL"].astype(str) == str(bbl)]
    if rows.empty:
        return None
    rp = rows.iloc[0].geometry.representative_point()
    lon, lat = get_stateplane_to_lonlat(rp.x, rp.y)
    return (float(lon), float(lat))
