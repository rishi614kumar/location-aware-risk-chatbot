import geopandas as gpd
from functools import lru_cache
import numpy as np
import fiona
from typing import List, Optional
from config.settings import MAPPLUTO_GDB_PATH


def load_lion(lion_path: str, layer: str = "lion") -> gpd.GeoDataFrame:
    """
    Load the NYC LION street network as a GeoDataFrame.
    Ensures the coordinate reference system (CRS) is EPSG:2263 (NAD83 / NY Long Island ftUS).
    """
    lion = gpd.read_file(lion_path, layer=layer)
    if lion.crs is None:
        lion.set_crs(epsg=2263, inplace=True)
    elif lion.crs.to_epsg() != 2263:
        lion = lion.to_crs(epsg=2263)
    print(f"✅ Loaded LION: {len(lion)} rows, CRS={lion.crs}")
    return lion


@lru_cache(maxsize=1)
def load_pluto(layer: Optional[str] = None) -> gpd.GeoDataFrame:
    """
    Load MapPLUTO (NYC tax lot dataset) from a .gdb or other geospatial source.
    Automatically uses the first layer in the file if none is specified.
    Converts CRS to EPSG:2263 if necessary.
    """
    # If no layer is provided, list and pick the first one
    if layer is None:
        layers = fiona.listlayers(MAPPLUTO_GDB_PATH)
        if not layers:
            raise ValueError(f"❌ No layers found in {MAPPLUTO_GDB_PATH}")
        layer = layers[0]
        print(f"ℹ️ No layer specified — using first layer: '{layer}'")

    # Read the selected layer
    pluto = gpd.read_file(MAPPLUTO_GDB_PATH, layer=layer)

    # Ensure CRS consistency
    if pluto.crs is None:
        pluto.set_crs(epsg=2263, inplace=True)
    elif pluto.crs.to_epsg() != 2263:
        pluto = pluto.to_crs(epsg=2263)

    # Check that the dataset has a valid BBL column
    if "BBL" not in pluto.columns:
        raise ValueError("❌ The PLUTO dataset does not contain a BBL field.")

    print(f"✅ Loaded PLUTO layer '{layer}' with {len(pluto)} rows, CRS={pluto.crs}")
    return pluto



def get_bbls_from_lion_span(
    lion: gpd.GeoDataFrame,
    pluto: gpd.GeoDataFrame,
    street_name: str
) -> List[str]:
    """
    Given a street name, return all BBLs (tax lots) that intersect it.
    Uses spatial join with buffered street geometries.
    """
    # Filter matching street segments (case-insensitive)
    subset = lion[lion["Street"].str.upper().str.contains(street_name.upper(), na=False)]
    if subset.empty:
        print(f"⚠️ No street found for: {street_name}")
        return []

    print(f"✅ Found {len(subset)} segments matching '{street_name}'")

    # Create per-segment buffer (simulating street width)
    subset = subset.copy()
    subset["buf_ft"] = np.where(
        subset["StreetWidth_Max"].isna(), 30, subset["StreetWidth_Max"] + 10
    )
    subset["buf_ft"] = np.clip(subset["buf_ft"], 10, 120)
    subset["geometry"] = subset.buffer(subset["buf_ft"], cap_style=2, join_style=2)

    # Spatial join between street buffers and tax lots
    joined = gpd.sjoin(pluto, subset, how="inner", predicate="intersects")

    unique_bbls = sorted(joined["BBL"].unique())
    print(f"🏙️ Found {len(unique_bbls)} tax lots intersecting {street_name}")
    return unique_bbls



def get_lion_span_from_bbl(
    lion: gpd.GeoDataFrame,
    pluto: gpd.GeoDataFrame,
    bbl: str
) -> Optional[str]:
    """
    Given a single BBL, return the street(s) it lies on.
    Performs a reverse spatial join between the lot polygon and LION street buffers.
    """
    target = pluto[pluto["BBL"] == int(bbl)]
    if target.empty:
        print(f"⚠️ No record found for BBL: {bbl}")
        return None

    # Build street buffers
    lion_buf = lion.copy()
    lion_buf["buf_ft"] = np.where(
        lion_buf["StreetWidth_Max"].isna(), 30, lion_buf["StreetWidth_Max"] + 10
    )
    lion_buf["buf_ft"] = np.clip(lion_buf["buf_ft"], 10, 120)
    lion_buf["geometry"] = lion_buf.buffer(lion_buf["buf_ft"], cap_style=2, join_style=2)

    # Find which street(s) intersect the target BBL
    joined = gpd.sjoin(target, lion_buf, how="inner", predicate="intersects")

    if joined.empty:
        print(f"⚠️ No streets found intersecting BBL {bbl}")
        return None

    street_names = sorted(joined["Street"].dropna().unique())
    return ", ".join(street_names)


# ============================================================
# Example Usage (for testing)
# ============================================================
if __name__ == "__main__":
    LION_PATH = "/Users/davinkey/Desktop/capstone/lion/lion.gdb"
    PLUTO_PATH = "/Users/davinkey/Desktop/capstone/nyc_mappluto_25v2_1_unclipped_fgdb/MapPLUTO25v2_1_unclipped.gdb"

    lion = load_lion(LION_PATH)
    pluto = load_pluto(PLUTO_PATH, layer="MapPLUTO_25v2_1_unclipped")

    # Example 1: From street → BBLs
    bbls = get_bbls_from_lion_span(lion, pluto, "EAST 168 STREET")
    print(f"Found {len(bbls)} BBLs\n")

    # Example 2: From BBL → Street(s)
    street = get_lion_span_from_bbl(lion, pluto, bbl="2023720020")
    print(f"BBL 2023720020 corresponds to: {street}")
