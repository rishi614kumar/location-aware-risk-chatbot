import os
import geopandas as gpd
import fiona
import pandas as pd
from __future__ import annotations
from typing import Optional, Tuple, List
from shapely.geometry import Point
from functools import lru_cache
from typing import Tuple
from config.settings import MAPPLUTO_GDB_PATH
from adapters.epsg import get_lonlat_to_stateplane, get_stateplane_to_lonlat

def _norm_bbl_str(boro_code, block, lot) -> Optional[str]:
    try:
        b = int(str(boro_code).strip())
        blk = int(str(block).split(".")[0])
        lt  = int(str(lot).split(".")[0])
        return f"{b}{blk:05d}{lt:04d}"
    except Exception:
        return None

@lru_cache(maxsize=1)
def _load_pluto_2263() -> gpd.GeoDataFrame:
    path = MAPPLUTO_GDB_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(f"MapPLUTO GDB not found: {path}")

    layers = fiona.listlayers(path)
    cands = [ly for ly in layers if "mappluto" in ly.lower()]
    layer = cands[0] if cands else layers[0]

    pluto = gpd.read_file(path, layer=layer)

    # Column name normalization (case-insensitive)
    cols = {c.lower(): c for c in pluto.columns}
    # Prefer composing BBL from parts to avoid float precision loss
    boro_col  = cols.get("borocode") or cols.get("boroughcode") or cols.get("boro")
    block_col = cols.get("block")
    lot_col   = cols.get("lot")

    if not (boro_col and block_col and lot_col):
        # fallback: try existing BBL column if parts are missing
        bbl_col = cols.get("bbl")
        if not bbl_col:
            raise ValueError("Could not find BBL parts (BoroCode/Block/Lot) or BBL column in PLUTO.")
        pluto["BBL"] = (
            pluto[bbl_col]
            .apply(lambda v: str(v).split(".")[0])  #  OK if integer-like
        )
    else:
        pluto["BBL"] = [
            _norm_bbl_str(pluto.loc[i, boro_col], pluto.loc[i, block_col], pluto.loc[i, lot_col])
            for i in pluto.index
        ]

    pluto["BBL"] = pluto["BBL"].astype(str)

    if "BBL" not in pluto.columns or "geometry" not in pluto.columns:
        raise ValueError("MapPLUTO must have 'BBL' and 'geometry' columns.")
    
    keep = [c for c in ["BBL", "geometry"] if c in pluto.columns]
    pluto = pluto[keep].dropna(subset=["geometry"]).copy()

    if pluto.crs is None:
        raise ValueError("MapPLUTO has no CRS; set it before use.")
    
    pluto_2263 = pluto.to_crs(2263)
    pluto_2263.sindex  # build spatial index
    return pluto_2263

def _pt2263(lon: float, lat: float) -> Point:
    x, y = get_lonlat_to_stateplane(lon, lat)
    return Point(x, y)

def get_bbl_from_lonlat(lon: float, lat: float) -> Optional[tuple[str, float]]:
    """Return (BBL, distance_ft) of the nearest MapPLUTO lot to the given lon/lat"""
    # polygons in EPSG:2263 (feet)
    pluto = _load_pluto_2263()
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

    pluto = _load_pluto_2263()

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

    pluto = _load_pluto_2263()
    rows = pluto[pluto["BBL"].astype(str) == str(bbl)]
    if rows.empty:
        return None
    rp = rows.iloc[0].geometry.representative_point()
    lon, lat = get_stateplane_to_lonlat(rp.x, rp.y)
    return (float(lon), float(lat))
