from __future__ import annotations
import geopandas as gpd
import numpy as np
from typing import List, Optional
from data.pluto import load_pluto_geom
from data.lion import load_lion_geom  

def get_bbls_from_lion_span(
    street_name: str
) -> List[str]:
    """
    Given a street name, return all BBLs (tax lots) that intersect it.
    Uses spatial join with buffered street geometries.
    """
    pluto = load_pluto_geom()
    lion = load_lion_geom()
    # Filter matching street segments (case-insensitive)
    subset = lion[lion["_street_name"].str.upper().str.contains(street_name.upper(), na=False)]
    if subset.empty:
        print(f"No street found for: {street_name}")
        return []

    print(f"Found {len(subset)} segments matching '{street_name}'")

    # Create per-segment buffer (simulating street width)
    subset = subset.copy()
    subset["buf_ft"] = np.where(
        subset["_width_ft"].isna(), 30, subset["_width_ft"] + 10
    )
    subset["buf_ft"] = np.clip(subset["buf_ft"], 10, 120)
    subset["geometry"] = subset.buffer(subset["buf_ft"], cap_style=2, join_style=2)

    # Spatial join between street buffers and tax lots
    joined = gpd.sjoin(pluto, subset, how="inner", predicate="intersects")

    unique_bbls = sorted(joined["BBL"].unique())
    print(f"Found {len(unique_bbls)} tax lots intersecting {street_name}")
    return unique_bbls



def get_lion_span_from_bbl(
    bbl: str
) -> Optional[str]:
    """
    Given a single BBL, return the street(s) it lies on.
    Performs a reverse spatial join between the lot polygon and LION street buffers.
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
    lion_buf["buf_ft"] = np.where(
        lion_buf["_width_ft"].isna(), 30, lion_buf["_width_ft"] + 10
    )
    lion_buf["buf_ft"] = np.clip(lion_buf["buf_ft"], 10, 120)
    lion_buf["geometry"] = lion_buf.buffer(lion_buf["buf_ft"], cap_style=2, join_style=2)

    # Find which street(s) intersect the target BBL
    joined = gpd.sjoin(target, lion_buf, how="inner", predicate="intersects")

    if joined.empty:
        print(f"No streets found intersecting BBL {bbl}")
        return None

    street_names = sorted(joined["_street_name"].dropna().unique())
    return ", ".join(street_names)
