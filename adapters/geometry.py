# adapters/geometry.py
from __future__ import annotations
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from typing import List, Optional, Tuple, Dict, Any
from config.logger import logger
from data.pluto import load_pluto_geom
from adapters.epsg import get_lonlat_to_stateplane


def _points_to_geodf(df: pd.DataFrame, lon_col: str, lat_col: str) -> gpd.GeoDataFrame:
    """
    Convert lon/lat dataframe to GeoDataFrame in EPSG:2263.
    """
    if lon_col not in df.columns or lat_col not in df.columns:
        raise ValueError(f"Missing columns {lon_col}, {lat_col} in df")

    # create 4326 points
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4326",
    )

    # project to stateplane 2263
    gdf = gdf.to_crs(2263)
    return gdf


def _build_query_polygon(target_bbls: List[str], buffer_ft: float = 25.0):
    """
    Given BBLs, return a single shapely polygon from MapPLUTO with buffer applied.
    """
    pluto = load_pluto_geom()

    lots = pluto[pluto["BBL"].astype(str).isin([str(b) for b in target_bbls])]
    if lots.empty:
        return None

    poly = lots.unary_all
    if buffer_ft > 0:
        poly = poly.buffer(buffer_ft)

    return poly


def filter_dataset_by_geometry(
    df: pd.DataFrame,
    target_bbls: List[str],
    lon_col: str,
    lat_col: str,
    buffer_ft: float = 25.0,
) -> pd.DataFrame:
    """
    Fallback spatial filter for datasets that cannot use BBL/NTA/Precinct filters.

    Steps:
      1. Convert dataset lon/lat → GeoDataFrame in EPSG:2263  
      2. Build union polygon from target BBLs  
      3. Return only rows whose points intersect the buffered polygon
    """

    if not target_bbls:
        logger.warning("No target BBLs passed to geometry filter — returning empty df.")
        return df.iloc[0:0]

    try:
        gdf = _points_to_geodf(df, lon_col, lat_col)
    except Exception as exc:
        logger.error(f"Failed to convert dataset to GeoDataFrame: {exc}")
        return df.iloc[0:0]

    poly = _build_query_polygon(target_bbls, buffer_ft)
    if poly is None:
        logger.warning("Could not build query polygon from BBLs — returning no rows.")
        return df.iloc[0:0]

    # Spatial intersection
    try:
        mask = gdf.intersects(poly)
        return gdf[mask].copy()
    except Exception as exc:
        logger.error(f"Spatial intersection failed: {exc}")
        return df.iloc[0:0]