from __future__ import annotations
import geopandas as gpd
import numpy as np
from typing import List, Optional, Union
from data.pluto import load_pluto_geom
from data.lion import load_lion_geom
from config.settings import (
    MAX_BUFFER_FT, MIN_BUFFER_FT, DEFAULT_BUFFER_INCREMENT_FT, DEFAULT_BUFFER_FT)


def get_bbls_from_lion_span(
    street_name: str,
    buffer_ft: Optional[Union[int, float]] = None
) -> List[str]:
    """
    Given a street name, return all BBLs (tax lots) that intersect it.
    Optionally allows a custom buffer distance (in feet).
    If buffer_ft is None, uses StreetWidth_Max + DEFAULT_BUFFER_INCREMENT_FT as default.
    """
    pluto = load_pluto_geom()
    lion = load_lion_geom()

    # Filter matching street segments (case-insensitive)
    subset = lion[lion["_street_name"].str.upper().str.contains(street_name.upper(), na=False)]
    if subset.empty:
        print(f"No street found for: {street_name}")
        return []

    print(f"Found {len(subset)} segments matching '{street_name}'")

    # Create per-segment buffer
    subset = subset.copy()
    if buffer_ft is None:
        # Default: StreetWidth_Max + DEFAULT_BUFFER_INCREMENT_FT ft
        subset["buf_ft"] = np.where(
            subset["_width_ft"].isna(), DEFAULT_BUFFER_FT, subset["_width_ft"] + DEFAULT_BUFFER_INCREMENT_FT
        )
        subset["buf_ft"] = np.clip(subset["buf_ft"], MIN_BUFFER_FT, MAX_BUFFER_FT)
    else:
        # Custom buffer
        subset["buf_ft"] = buffer_ft

    subset["geometry"] = subset.buffer(subset["buf_ft"], cap_style=2, join_style=2)

    # Spatial join between street buffers and tax lots
    joined = gpd.sjoin(pluto, subset, how="inner", predicate="intersects")

    unique_bbls = sorted(joined["BBL"].unique())
    print(f"Found {len(unique_bbls)} tax lots intersecting {street_name}")
    return unique_bbls


def get_lion_span_from_bbl(
    bbl: str,
    buffer_ft: Optional[Union[int, float]] = None
) -> Optional[str]:
    """
    Given a single BBL, return the street(s) it lies on.
    Optionally allows a custom buffer distance (in feet).
    If buffer_ft is None, uses StreetWidth_Max + DEFAULT_BUFFER_INCREMENT_FT ft as default.
    """
    lion = load_lion_geom()
    pluto = load_pluto_geom()

    # Ensure BBL is compared as string
    target = pluto[pluto["BBL"] == str(bbl)]
    if target.empty:
        print(f"No record found for BBL: {bbl}")
        return None

    # Ensure CRS matches between lion and pluto
    if lion.crs.to_epsg() != pluto.crs.to_epsg():
        lion = lion.to_crs(pluto.crs)

    # Build street buffers
    lion_buf = lion.copy()
    if buffer_ft is None:
        # Default buffer logic
        lion_buf["buf_ft"] = np.where(
            lion_buf["_width_ft"].isna(), DEFAULT_BUFFER_FT, lion_buf["_width_ft"] + DEFAULT_BUFFER_INCREMENT_FT
        )
        lion_buf["buf_ft"] = np.clip(lion_buf["buf_ft"], MIN_BUFFER_FT, MAX_BUFFER_FT)
    else:
        # Custom buffer
        lion_buf["buf_ft"] = buffer_ft

    lion_buf["geometry"] = lion_buf.buffer(lion_buf["buf_ft"], cap_style=2, join_style=2)

    # Find which street(s) intersect the target BBL
    joined = gpd.sjoin(target, lion_buf, how="inner", predicate="intersects")

    if joined.empty:
        print(f"No streets found intersecting BBL {bbl}")
        return None

    street_names = sorted(joined["_street_name"].dropna().unique())
    return ", ".join(street_names)
