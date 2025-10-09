# adapters/data/lion.py
from __future__ import annotations
import os
from functools import lru_cache
from typing import Optional, Tuple

import fiona
import geopandas as gpd
import numpy as np

from config.settings import LION_GDB_PATH 

# ------------------------------
# Internal helpers
# ------------------------------

def _pick_layer(path: str, prefer_substr: str = "lion") -> str:
    layers = fiona.listlayers(path)
    if not layers:
        raise ValueError(f"No layers found in {path}")
    cands = [ly for ly in layers if prefer_substr.lower() in ly.lower()]
    return cands[0] if cands else layers[0]

def _ensure_2263(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        gdf.set_crs(2263, inplace=True)
    elif gdf.crs.to_epsg() != 2263:
        gdf = gdf.to_crs(2263)
    return gdf

def _normalize_columns(lion: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Standardize likely street name and width columns to _street_name / _width_ft.
    We keep original columns too—this only adds standardized ones.
    """
    cols = {c.lower(): c for c in lion.columns}

    # Street name candidates differ by export
    street_col = (
        cols.get("street")
        or cols.get("streetname")
        or cols.get("fullname")
        or cols.get("full_stree")
        or cols.get("street_nam")
        or cols.get("st_name")
    )
    if street_col:
        lion["_street_name"] = lion[street_col].astype(str).str.strip()
    else:
        lion["_street_name"] = None  # still usable for reverse-geo ops

    # Right-of-way width candidates
    width_col = (
        cols.get("streetwidth_max")
        or cols.get("rw_width_max")
        or cols.get("rw_width")
        or cols.get("width")
    )
    lion["_width_ft"] = lion[width_col].astype(float) if width_col else np.nan

    return lion

# ------------------------------
# Single disk-read base (cached)
# ------------------------------

@lru_cache(maxsize=4)  # cache per layer key
def _load_lion_base(layer: Optional[str] = None) -> gpd.GeoDataFrame:
    """
    Read LION once (per layer), normalize CRS to EPSG:2263, add standardized columns,
    and build a spatial index.
    """
    path = LION_GDB_PATH
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"LION GDB not found: {path}")

    lyr = layer or _pick_layer(path, prefer_substr="lion")
    lion = gpd.read_file(path, layer=lyr)
    lion = _ensure_2263(lion)
    lion = _normalize_columns(lion)
    lion.sindex  # build spatial index
    return lion

# ------------------------------
# Public “views” (no extra I/O)
# ------------------------------

@lru_cache(maxsize=4)
def load_lion_geom(layer: Optional[str] = None) -> gpd.GeoDataFrame:
    """
    Geometry-only view with standardized helper columns:
      returns columns ['_street_name', '_width_ft', 'geometry']
    """
    gdf = _load_lion_base(layer)
    keep = ["_street_name", "_width_ft", "geometry"]
    return gdf[keep].copy()

@lru_cache(maxsize=4)
def load_lion_names(layer: Optional[str] = None) -> gpd.GeoDataFrame:
    """
    Lightweight view for name-only lookups (no geometry ops):
      returns ['_street_name'] (duplicates dropped)
    """
    gdf = _load_lion_base(layer)
    return gdf[["_street_name"]].drop_duplicates().copy()

@lru_cache(maxsize=4)
def load_lion_full(layer: Optional[str] = None) -> gpd.GeoDataFrame:
    """
    Full standardized frame (attributes + geometry), cached. Use sparingly.
    """
    return _load_lion_base(layer)
