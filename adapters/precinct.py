from __future__ import annotations
import geopandas as gpd
import fiona
from functools import lru_cache
from typing import Optional, Dict, List
from config.settings import MAPPLUTO_GDB_PATH

def _norm_bbl_str(boro_code, block, lot) -> Optional[str]:
    try:
        b = int(str(boro_code).strip())
        blk = int(str(block).split(".")[0])
        lt  = int(str(lot).split(".")[0])
        return f"{b}{blk:05d}{lt:04d}"
    except Exception:
        return None


@lru_cache(maxsize=1)
def _load_pluto() -> gpd.GeoDataFrame:
    if not MAPPLUTO_GDB_PATH:
        raise RuntimeError("MAPPLUTO_GDB_PATH not set in environment")

    layers = fiona.listlayers(MAPPLUTO_GDB_PATH)
    layer_name = [lyr for lyr in layers if "pluto" in lyr.lower()][0]
    gdf = gpd.read_file(MAPPLUTO_GDB_PATH, layer=layer_name)

    # Column name normalization (case-insensitive)
    cols = {c.lower(): c for c in gdf.columns}
    # Prefer composing BBL from parts to avoid float precision loss
    boro_col  = cols.get("borocode") or cols.get("boroughcode") or cols.get("boro")
    block_col = cols.get("block")
    lot_col   = cols.get("lot")
    prct_col  = cols.get("policeprct") or cols.get("police_prct") or cols.get("policepct")

    if not (boro_col and block_col and lot_col):
        # fallback: try existing BBL column if parts are missing
        bbl_col = cols.get("bbl")
        if not bbl_col:
            raise ValueError("Could not find BBL parts (BoroCode/Block/Lot) or BBL column in PLUTO.")
        gdf["BBL"] = (
            gdf[bbl_col]
            .apply(lambda v: str(v).split(".")[0])  #  OK if integer-like
        )
    else:
        gdf["BBL"] = [
            _norm_bbl_str(gdf.loc[i, boro_col], gdf.loc[i, block_col], gdf.loc[i, lot_col])
            for i in gdf.index
        ]

    if not prct_col:
        raise ValueError("Could not find PolicePrct column in PLUTO.")

    gdf = gdf.rename(columns={prct_col: "PolicePrct"})
    # Ensure types are clean
    gdf["PolicePrct"] = gdf["PolicePrct"].astype("Int64")  # nullable int
    gdf["BBL"] = gdf["BBL"].astype(str)

    # Keep only what we need for fast lookups
    return gdf[["BBL", "PolicePrct"]].dropna(subset=["BBL"])

# ---- public API ----
def get_precinct_from_bbl(bbl: str) -> Optional[int]:
    """
    Returns the precinct number for a BBL (int) or None if missing.
    """
    pluto = _load_pluto()
    row = pluto.loc[pluto["BBL"] == bbl]
    if row.empty:
        return None
    val = row.iloc[0]["PolicePrct"]
    return int(val) if val is not None else None

def get_bbls_from_precinct(precinct_number: int) -> List[str]:
    """
    Returns all BBLs within a given precinct as a list[str].
    """
    pluto = _load_pluto()
    sub = pluto.loc[pluto["PolicePrct"] == precinct_number, "BBL"]
    return [str(b) for b in sub.unique()]