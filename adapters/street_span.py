from __future__ import annotations
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Union

import geopandas as gpd
import numpy as np
from shapely.geometry import LineString, MultiLineString
from shapely.ops import unary_union

from data.pluto import load_pluto_geom
from data.lion import load_lion_geom, load_lion14_geom, load_lion_full
from config.settings import (
    MAX_BUFFER_FT, MIN_BUFFER_FT, DEFAULT_BUFFER_INCREMENT_FT, DEFAULT_BUFFER_FT)


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _build_lion_buffer(lion: gpd.GeoDataFrame, buffer_ft: Optional[Union[int, float]]) -> gpd.GeoDataFrame:
    lion = lion.copy()
    if buffer_ft is None:
        lion["buf_ft"] = np.where(
            lion["_width_ft"].isna(),
            DEFAULT_BUFFER_FT,
            lion["_width_ft"] + DEFAULT_BUFFER_INCREMENT_FT,
        )
        lion["buf_ft"] = np.clip(lion["buf_ft"], MIN_BUFFER_FT, MAX_BUFFER_FT)
    else:
        lion["buf_ft"] = float(buffer_ft)

    lion["geometry"] = lion.buffer(lion["buf_ft"], cap_style=2, join_style=2)
    return lion


def _extract_endpoints(geom: object) -> Optional[Tuple[Tuple[float, float], Tuple[float, float]]]:
    """Return rounded start/end coordinates for a LION segment geometry."""

    if geom is None:
        return None
    if isinstance(geom, MultiLineString):
        if not geom.geoms:
            return None
        line = max(geom.geoms, key=lambda g: g.length)
    elif isinstance(geom, LineString):
        line = geom
    else:
        return None

    coords = list(line.coords)
    if len(coords) < 2:
        return None

    def _round(pt: Tuple[float, float]) -> Tuple[float, float]:
        return (round(pt[0], 1), round(pt[1], 1))

    return _round(coords[0]), _round(coords[-1])


def _build_segment_graph(lion: gpd.GeoDataFrame) -> Tuple[Dict[int, Tuple[Tuple[float, float], Tuple[float, float]]], Dict[Tuple[float, float], List[int]]]:
    segment_nodes: Dict[int, Tuple[Tuple[float, float], Tuple[float, float]]] = {}
    node_to_segments: Dict[Tuple[float, float], List[int]] = defaultdict(list)

    for idx, geom in lion.geometry.items():
        endpoints = _extract_endpoints(geom)
        if not endpoints:
            continue
        segment_nodes[idx] = endpoints
        for node in endpoints:
            node_to_segments[node].append(idx)

    return segment_nodes, node_to_segments


def _bfs_segment_path(
    start_seg: int,
    target_seg: int,
    segment_nodes: Dict[int, Tuple[Tuple[float, float], Tuple[float, float]]],
    node_to_segments: Dict[Tuple[float, float], List[int]],
) -> List[int]:
    if start_seg == target_seg:
        return [start_seg]

    visited = {start_seg}
    parents: Dict[int, Optional[int]] = {start_seg: None}
    queue: deque[int] = deque([start_seg])

    while queue:
        seg = queue.popleft()
        endpoints = segment_nodes.get(seg)
        if not endpoints:
            continue
        for node in endpoints:
            for neighbor in node_to_segments.get(node, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                parents[neighbor] = seg
                if neighbor == target_seg:
                    path: List[int] = [neighbor]
                    while parents[path[-1]] is not None:
                        path.append(parents[path[-1]])  # type: ignore[arg-type]
                    return list(reversed(path))
                queue.append(neighbor)

    return []


def _shortest_segment_path(
    segment_nodes: Dict[int, Tuple[Tuple[float, float], Tuple[float, float]]],
    node_to_segments: Dict[Tuple[float, float], List[int]],
    start_candidates: List[int],
    end_candidates: List[int],
) -> List[int]:
    best: List[int] = []

    for start_seg in start_candidates:
        for end_seg in end_candidates:
            path = _bfs_segment_path(start_seg, end_seg, segment_nodes, node_to_segments)
            if path and (not best or len(path) < len(best)):
                best = path

    return best


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


def get_bbls_between_intersections(
    start_bbl: str,
    end_bbl: str,
    *,
    street_name: Optional[str] = None,
    buffer_ft: Optional[Union[int, float]] = None,
) -> List[str]:
    """
    Given two endpoint BBLs (usually intersections), walk the LION network for that
    street and return all BBLs that touch the street span between them.
    """

    pluto = load_pluto_geom()
    lion = load_lion_full()

    start = pluto[pluto["BBL"] == str(start_bbl)]
    end = pluto[pluto["BBL"] == str(end_bbl)]
    if start.empty or end.empty:
        print(f"⚠️ Missing endpoint geometry for span: {start_bbl} or {end_bbl}")
        return []

    # Keep CRS aligned
    if lion.crs.to_epsg() != pluto.crs.to_epsg():
        lion = lion.to_crs(pluto.crs)

    # Optional: constrain to the named street if provided
    lion_main = lion
    if street_name:
        mask = lion_main["_street_name"].str.upper().str.contains(str(street_name).upper(), na=False)
        filtered = lion_main[mask]
        if not filtered.empty:
            lion_main = filtered

    if lion_main.empty:
        print(f"⚠️ No street segments found for '{street_name}'")
        return []

    start_point = start.geometry.centroid.iloc[0]
    end_point = end.geometry.centroid.iloc[0]

    start_candidates = lion_main.geometry.distance(start_point).nsmallest(min(3, len(lion_main))).index.tolist()
    end_candidates = lion_main.geometry.distance(end_point).nsmallest(min(3, len(lion_main))).index.tolist()

    segment_nodes, node_to_segments = _build_segment_graph(lion_main)
    path_ids = _shortest_segment_path(segment_nodes, node_to_segments, start_candidates, end_candidates)

    if not path_ids:
        # Fallback: at least include the closest segments to each endpoint
        core_ids = _dedupe_preserve_order([*start_candidates, *end_candidates])
        path_ids = [seg for seg in core_ids if seg in lion_main.index]

    path_segments = lion_main.loc[path_ids]

    if path_segments.empty:
        buffer_dist = float(buffer_ft or DEFAULT_BUFFER_FT)
        straight_line = LineString([start_point, end_point])
        scope_geom = straight_line.buffer(buffer_dist, cap_style=2, join_style=2)
        scope_gdf = gpd.GeoDataFrame(geometry=[scope_geom], crs=pluto.crs)
        joined = gpd.sjoin(pluto, scope_gdf, how="inner", predicate="intersects")
        return sorted(joined["BBL"].astype(str).unique())

    buffered = _build_lion_buffer(path_segments, buffer_ft)
    scope_geom = unary_union(buffered.geometry.values)
    scope_gdf = gpd.GeoDataFrame(geometry=[scope_geom], crs=pluto.crs)
    joined = gpd.sjoin(pluto, scope_gdf, how="inner", predicate="intersects")
    bbls = sorted(joined["BBL"].astype(str).unique())

    if bbls:
        print(f"✅ Street span between {start_bbl} and {end_bbl} covers {len(bbls)} BBLs")
        return bbls

    # If the buffered path failed to intersect PLUTO, fall back to a simple corridor
    buffer_dist = float(buffer_ft or DEFAULT_BUFFER_FT)
    straight_line = LineString([start_point, end_point])
    scope_geom = straight_line.buffer(buffer_dist, cap_style=2, join_style=2)
    scope_gdf = gpd.GeoDataFrame(geometry=[scope_geom], crs=pluto.crs)
    joined = gpd.sjoin(pluto, scope_gdf, how="inner", predicate="intersects")
    fallback_bbls = sorted(joined["BBL"].astype(str).unique())
    if fallback_bbls:
        print(f"⚠️ Using fallback corridor between {start_bbl} and {end_bbl}; {len(fallback_bbls)} BBLs found")
    return fallback_bbls


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

def get_segment_id_from_bbl(
    bbl: str,
    buffer_ft: Optional[Union[int, float]] = None
) -> Optional[List[str]]:
    """
    Given a single BBL, return the street segment IDs it lies on.
    Optionally allows a custom buffer distance (in feet).
    If buffer_ft is None, uses StreetWidth_Max + DEFAULT_BUFFER_INCREMENT_FT ft as default.
    """
    lion = load_lion14_geom()
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

    segment_ids = sorted(joined["SegmentID"].dropna().unique())
    print(f"Found segment IDs for BBL {bbl}: {segment_ids}")
    return segment_ids