# adapters/surrounding.py
from __future__ import annotations
from typing import List, Literal, Optional, Union
from functools import lru_cache

import geopandas as gpd
import numpy as np
from shapely.ops import unary_union

from data.pluto import load_pluto_geom
from data.lion import load_lion_geom

from config.settings import (
    MAX_BUFFER_FT, MIN_BUFFER_FT, DEFAULT_BUFFER_INCREMENT_FT, DEFAULT_BUFFER_FT)



def _build_lion_buffer(lion: gpd.GeoDataFrame, street_buffer_ft: Optional[Union[int, float]]) -> gpd.GeoDataFrame:
    """
    Create per-segment buffer for LION.
    If street_buffer_ft is None, use width-based default: _width_ft + 10 (clipped 10~120).
    Otherwise use the fixed custom buffer.
    """
    lion = lion.copy()
    if street_buffer_ft is None:
        # default: width + 10ft, with guards
        lion["buf_ft"] = np.where(lion["_width_ft"].isna(), DEFAULT_BUFFER_FT, lion["_width_ft"] + DEFAULT_BUFFER_INCREMENT_FT)
        lion["buf_ft"] = np.clip(lion["buf_ft"], MIN_BUFFER_FT, MAX_BUFFER_FT)
    else:
        lion["buf_ft"] = float(street_buffer_ft)

    lion["geometry"] = lion.buffer(lion["buf_ft"], cap_style=2, join_style=2)
    return lion


def get_surrounding_bbls_from_bbl(
    bbl: str,
    *,
    mode: Literal["radius", "street"],
    radius_ft: Optional[Union[int, float]] = None,
    street_buffer_ft: Optional[Union[int, float]] = None,
    include_self: bool = False,
) -> List[str]:
    """
    Given a BBL, return nearby/surrounding BBLs using two geo-scoping modes.

    Parameters
    ----------
    bbl : str
        Input tax lot BBL (string).
    mode : {'radius', 'street'}
        - 'radius': buffer the *lot polygon* by `radius_ft` feet, return intersecting BBLs.
        - 'street': find LION street segments that intersect the *lot polygon* (with a street buffer),
                    then return BBLs intersecting those buffered segments (i.e., along-street scope).
    radius_ft : float, optional
        Search radius for mode='radius'. Required when mode='radius'.
    street_buffer_ft : float, optional
        Street buffer for mode='street'. If None, uses width-based default (_width_ft + 10, clipped 10~120).
    include_self : bool
        Whether to include the input BBL in the results. Default False.

    Returns
    -------
    List[str] : sorted unique BBLs
    """
    # Load data once (cached)
    pluto = _cached_pluto_geom()          # ['BBL','geometry'] in EPSG:2263
    lion = _cached_lion_geom()            # ['_street_name','_width_ft','geometry'] in EPSG:2263


    # Grab target lot
    target = pluto[pluto["BBL"] == str(bbl)]
    if target.empty:
        print(f"⚠️ No PLUTO record found for BBL {bbl}")
        return []

    if mode == "radius":
        if radius_ft is None:
            raise ValueError("mode='radius' requires radius_ft.")

        # Buffer the *lot polygon* by radius (feet)
        scope = unary_union(target.buffer(float(radius_ft)))
        scope_gdf = gpd.GeoDataFrame(geometry=[scope], crs=pluto.crs)

        # Intersect with all PLUTO lots
        joined = gpd.sjoin(pluto, scope_gdf, how="inner", predicate="intersects")

        # Compute centroid distance (in feet)
        target_center = target.geometry.centroid.iloc[0]
        joined["distance_ft"] = joined.geometry.centroid.distance(target_center)

        # Sort by distance ascending
        joined = joined.sort_values("distance_ft")

        # Extract BBLs
        bbls = joined["BBL"].astype(str).tolist()

        if not include_self:
            bbls = [b for b in bbls if b != str(bbl)]

    elif mode == "street":
        # Step 1: Build street buffers (width-based or custom)
        lion_buf = _build_lion_buffer(lion, street_buffer_ft)

        # Step 2: Select only those LION segments whose buffer intersects the *target lot polygon*
        # This anchors you to the exact “street span” touching the lot, not the whole street name.
        hit_segments = gpd.sjoin(lion_buf, target[["geometry"]], how="inner", predicate="intersects")
        if hit_segments.empty:
            print(f"⚠️ No street segments (buffered) intersect BBL {bbl}")
            return [] if include_self else []
        

        cols_to_show = []
        if "SegmentID" in hit_segments.columns:
            cols_to_show.append("SegmentID")
        if "_street_name" in hit_segments.columns:
            cols_to_show.append("_street_name")

        print(f"\n✅ Found {len(hit_segments)} street segment(s) touching BBL {bbl}:")
        if cols_to_show:
            # Print SegmentID + street name in a clean table
            print(hit_segments[cols_to_show].drop_duplicates().to_string(index=False))
        else:
            print("⚠️ Could not find SegmentID or _street_name columns in LION layer")

            

        # Optional: union these buffer geometries to form one scope polygon
        scope_geom = unary_union(hit_segments.geometry.values)
        scope_gdf = gpd.GeoDataFrame(geometry=[scope_geom], crs=pluto.crs)

        # Step 3: Intersect PLUTO with the street-scope buffer to get along-street BBLs
        joined = gpd.sjoin(pluto, scope_gdf, how="inner", predicate="intersects")
        bbls = set(joined["BBL"].astype(str).tolist())

    else:
        raise ValueError("mode must be either 'radius' or 'street'.")

    # Include/exclude self
    if not include_self:
        bbls = [b for b in bbls if b != str(bbl)]

    # Ensure consistent sorted list output
    if mode == "radius":
    # Already sorted by distance — keep order
        pass
    else:
        # For 'street' mode, just sort lexicographically for stability
        bbls = sorted(bbls)

    return bbls



@lru_cache(maxsize=1)
def _cached_pluto_geom() -> gpd.GeoDataFrame:
    """Load PLUTO geometry once and reuse across calls."""
    return load_pluto_geom()


@lru_cache(maxsize=1)
def _cached_lion_geom() -> gpd.GeoDataFrame:
    """Load LION geometry once and reuse across calls."""
    return load_lion_geom()



