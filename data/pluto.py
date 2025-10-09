from __future__ import annotations
import os
from functools import lru_cache
from typing import Optional
import fiona
import geopandas as gpd
from config.settings import MAPPLUTO_GDB_PATH

# ---------- internals ----------

def _pick_layer(path: str, prefer_substr: str = "mappluto") -> str:
    layers = fiona.listlayers(path)
    if not layers:
        raise ValueError(f"No layers found in {path}")
    cands = [ly for ly in layers if prefer_substr.lower() in ly.lower()]
    return cands[0] if cands else layers[0]

def _norm_bbl_str(boro_code, block, lot) -> Optional[str]:
    try:
        b = int(str(boro_code).strip())
        blk = int(str(block).split(".")[0])
        lt  = int(str(lot).split(".")[0])
        return f"{b}{blk:05d}{lt:04d}"
    except Exception:
        return None

def _compose_bbl(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    cols = {c.lower(): c for c in gdf.columns}
    boro  = cols.get("borocode") or cols.get("boroughcode") or cols.get("boro")
    block = cols.get("block")
    lot   = cols.get("lot")
    if boro and block and lot:
        gdf["BBL"] = [
            _norm_bbl_str(gdf.loc[i, boro], gdf.loc[i, block], gdf.loc[i, lot])
            for i in gdf.index
        ]
    else:
        bbl_col = cols.get("bbl")
        if not bbl_col:
            raise ValueError("PLUTO missing BBL and/or its parts (BoroCode/Block/Lot).")
        gdf["BBL"] = gdf[bbl_col].astype(str).str.split(".").str[0]
    gdf["BBL"] = gdf["BBL"].astype(str)
    return gdf

def _ensure_2263(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        gdf.set_crs(epsg=2263, inplace=True)
    elif gdf.crs.to_epsg() != 2263:
        gdf = gdf.to_crs(2263)
    return gdf

# ---------- single disk read ----------
@lru_cache(maxsize=1)
def _load_pluto_base() -> gpd.GeoDataFrame:
    """Reads PLUTO from disk ONCE, composes BBL, converts to EPSG:2263, returns full GeoDataFrame."""
    path = MAPPLUTO_GDB_PATH
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"MapPLUTO GDB not found: {path}")
    layer = _pick_layer(path)

    gdf = gpd.read_file(path, layer=layer)
    gdf = _compose_bbl(gdf)
    gdf = _ensure_2263(gdf)
    # build spatial index once; keeps a reference attached to the GeoDataFrame
    gdf.sindex
    return gdf

# ---------- lightweight “views” (no extra disk reads) ----------
@lru_cache(maxsize=1)
def load_pluto_lookup() -> gpd.GeoDataFrame:
    """Returns ['BBL', 'PolicePrct'] when available; all in-memory from the base frame."""
    gdf = _load_pluto_base()
    cols = {c.lower(): c for c in gdf.columns}
    prct = cols.get("policeprct") or cols.get("police_prct") or cols.get("policepct")
    if prct:
        out = gdf.rename(columns={prct: "PolicePrct"})[["BBL", "PolicePrct"]].copy()
        out["PolicePrct"] = out["PolicePrct"].astype("Int64")
        return out
    return gdf[["BBL"]].copy()

@lru_cache(maxsize=1)
def load_pluto_geom() -> gpd.GeoDataFrame:
    """Returns ['BBL', 'geometry'] view; no new file I/O."""
    gdf = _load_pluto_base()
    if "geometry" not in gdf.columns:
        raise ValueError("PLUTO layer has no 'geometry' column.")
    out = gdf[["BBL", "geometry"]].dropna(subset=["geometry"]).copy()
    out.sindex  # ensure spatial index present on the view
    return out

@lru_cache(maxsize=1)
def load_pluto_full() -> gpd.GeoDataFrame:
    """Returns the full standardized frame (same object as base, but cached accessor for consistency)."""
    return _load_pluto_base()
