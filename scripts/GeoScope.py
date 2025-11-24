"""
GeoScope stub: returns a filter dict for each dataset.
Replace logic here to generate spatial filters based on addresses and handler.datasets.
"""
from __future__ import annotations
import atexit
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
from itertools import combinations
from api.GeoClient import get_bbl_from_address, get_bbl_from_intersection
from dataclasses import dataclass
from adapters.surrounding import get_surrounding_bbls_from_bbl
from adapters.coords import get_lonlat_from_bbl
from adapters.precinct import get_precinct_from_bbl
from adapters.nta import get_nta_from_bbl
from adapters.street_span import get_lion_span_from_bbl
from adapters.schemas import GeoBundle
from scripts.GeoBundle import geo_from_bbl
from config.settings import DATASET_CONFIG,BORO_CODE_MAP
from config.logger import logger

try:
    _GEO_MAX_WORKERS = max(1, int(os.getenv("GEO_MAX_WORKERS", "4")))
except ValueError:
    _GEO_MAX_WORKERS = 4
_GEO_EXECUTOR = ThreadPoolExecutor(max_workers=_GEO_MAX_WORKERS)


def _shutdown_geo_executor() -> None:
    try:
        _GEO_EXECUTOR.shutdown(wait=False)
    except Exception:
        pass


atexit.register(_shutdown_geo_executor)


@dataclass(frozen=True)
class GeoResolution:
    """Resolved BBLs alongside their enriched GeoBundle payloads."""

    bundles: List[GeoBundle]
    bbls: List[str]


def _dedupe_preserve_order(values: List[Optional[str]]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _format_lonlat(lon: float, lat: float) -> str:
    return f"{lon:.6f},{lat:.6f}"

def _resolve_single_bbl(record: Dict[str, Any]) -> Optional[str]:
    house = (record.get("house_number") or "").strip()
    street = (record.get("street_name") or "").strip()
    borough = (record.get("borough") or "").strip()

    if not street or not borough:
        logger.warning("Incomplete address data; cannot resolve BBL for record.")
        return None

    normalized = street.replace(" and ", " & ").replace(" AND ", " & ")

    if "&" in normalized:
        try:
            street_one, street_two = [s.strip() for s in normalized.split("&", 1)]
            bbl_intersection = get_bbl_from_intersection(street_one, street_two, borough)
            logger.info(f"Geoclient intersection lookup: {street_one} & {street_two}, {borough} → BBL {bbl_intersection}")
            return bbl_intersection
        except Exception as exc:
            logger.warning(f"Intersection lookup failed for {normalized} ({borough}): {exc}")
            return None

    if house:
        try:
            bbl_address = get_bbl_from_address(f"{house} {street}", borough)
            logger.info(f"Geoclient lookup: {house} {street}, {borough} → BBL {bbl_address}")
            return bbl_address
        except Exception as exc:
            logger.warning(f"Address lookup failed for {house} {street} ({borough}): {exc}")

    return None


def resolve_geo_bundles_from_addresses(addresses: List[Dict[str, Any]]) -> GeoResolution:
    """Resolve address dicts into GeoBundle objects plus ordered BBL list."""

    bundles: List[GeoBundle] = []
    ordered_bbls: List[str] = []
    seen_bbls: set[str] = set()

    for record in addresses or []:
        bbl = _resolve_single_bbl(record)
        if not bbl:
            continue
        bbl_str = str(bbl)
        if bbl_str in seen_bbls:
            continue
        seen_bbls.add(bbl_str)

        try:
            bundle = geo_from_bbl(bbl_str)
            if not isinstance(bundle, GeoBundle):
                raise TypeError("geo_from_bbl did not return GeoBundle")
            if not bundle.bbl:
                bundle = bundle.copy(update={"bbl": bbl_str})
            bundles.append(bundle)
            ordered_bbls.append(bundle.bbl)
        except Exception as exc:
            logger.warning(f"Geo bundle lookup failed for BBL {bbl_str}: {exc}")
            ordered_bbls.append(bbl_str)

    ordered_bbls = _dedupe_preserve_order(ordered_bbls)
    return GeoResolution(bundles=bundles, bbls=ordered_bbls)


def get_surrounding_units(bbl_list: List[str], geo_unit: str, *, bundle_lookup: Optional[Dict[str, GeoBundle]] = None) -> List[str]:
    """
    Given a list of nearby BBLs, convert all to the specified geo_unit and
    deduplicate results.
    """
    units = set()

    for b in bbl_list:
        bundle = bundle_lookup.get(b) if bundle_lookup else None
        try:
            if geo_unit == "PRECINCT":
                if bundle and bundle.precinct:
                    val = str(bundle.precinct)
                else:
                    val = get_precinct_from_bbl(b)
                    val = str(val) if val is not None else None
            elif geo_unit.startswith("NTA"):
                if bundle and bundle.nta:
                    val = str(bundle.nta)
                else:
                    val = get_nta_from_bbl(b)
            elif geo_unit == "STREETSPAN":
                vals = get_lion_span_from_bbl(b)
                if vals:
                    units.update(vals)
                continue
            elif geo_unit in ("LONLAT", "COORD"):
                if bundle and bundle.longitude is not None and bundle.latitude is not None:
                    val = _format_lonlat(float(bundle.longitude), float(bundle.latitude))
                else:
                    coords = get_lonlat_from_bbl(b)
                    if coords is not None:
                        lon, lat = coords
                        val = _format_lonlat(lon, lat)
                    else:
                        val = None
            elif geo_unit == "BBL":
                val = b
            else:
                val = None

            if val:
                units.add(val)
        except Exception as e:
            logger.warning(f"Surrounding conversion failed for {b} ({geo_unit}): {e}")
            continue

    return list(units)

# Helper functions to modularize dataset filter generation

def resolve_bbls_from_addresses(addresses: List[Dict[str, Any]]) -> List[str]:
    """Resolve BBLs from a list of address dicts. Deduplicated, order-preserving."""

    resolution = resolve_geo_bundles_from_addresses(addresses)
    return resolution.bbls


def aggregate_surrounding_bbls(resolved_bbls: List[str], surrounding: bool = True) -> List[str]:
    """Aggregate surrounding BBLs for each resolved BBL when surrounding=True; otherwise return resolved list."""
    if not resolved_bbls:
        return []
    if not surrounding:
        return _dedupe_preserve_order(resolved_bbls)

    nearby_set = set()

    futures = {
        _GEO_EXECUTOR.submit(
            get_surrounding_bbls_from_bbl,
            bbl,
            mode="street",
            include_self=True,
        ): bbl
        for bbl in resolved_bbls
    }

    for future in as_completed(futures):
        seed_bbl = futures[future]
        try:
            results = future.result()
        except Exception as exc:
            logger.warning(f"Surrounding lookup failed for {seed_bbl}: {exc}")
            results = [seed_bbl]

        for item in results:
            nearby_set.add(item)
    nearby_bbls = sorted(nearby_set)
    logger.info(f"Aggregated {len(nearby_bbls)} unique BBLs from {len(resolved_bbls)} addresses")
    return nearby_bbls


def _build_where_for_geo_unit(
    geo_unit: str,
    bbls_to_use: List[str],
    borough_type: str, borough_form: int,col_name: Dict, col_digit: Dict,
    *,
    bundle_lookup: Optional[Dict[str, GeoBundle]] = None,
) -> Optional[str]:
    """Build a Socrata where clause for a specific geo unit given a list of BBLs to use."""
    geo_unit = (geo_unit or "BBL").upper()
    if not bbls_to_use:
        return None

    if geo_unit == "BBL":
        vals = ",".join(f"'{b}'" for b in bbls_to_use)
        return f"BBL IN ({vals})"

    if geo_unit == "PRECINCT":
        ids = get_surrounding_units(bbls_to_use, geo_unit, bundle_lookup=bundle_lookup)
        vals = ",".join(f"'{p}'" for p in ids)
        return f"PCT IN [{vals}]" if vals else None

    if geo_unit.startswith("NTA"):
        ids = get_surrounding_units(bbls_to_use, geo_unit, bundle_lookup=bundle_lookup)
        vals = ",".join(f"'{n}'" for n in ids)
        return f"nta_2020 IN ({vals})" if vals else None

    if geo_unit == "STREETSPAN":
        ids = get_surrounding_units(bbls_to_use, geo_unit, bundle_lookup=bundle_lookup)
        vals = ",".join(f"'{s}'" for s in ids)
        return f"SegmentID IN ({vals})" if vals else None

    if geo_unit in ("LONLAT", "COORD"):
        ids = get_surrounding_units(bbls_to_use, geo_unit, bundle_lookup=bundle_lookup)
        coords = [lonlat.split(',') for lonlat in ids]
        geometry_col = col_name.get("geometry", None)
        conds = " OR ".join([
            f"within_circle({geometry_col},{lat}, {lon}, 100)"
            for lon, lat in coords
        ])
        return conds or None
    
    if geo_unit == "BOROUGH":
        boro_list = []
        for full_bbl in bbls_to_use:
            try:
                s = str(full_bbl)
                boro = s[0]
                if borough_type.isalpha():
                    boro_options = BORO_CODE_MAP.get(boro, [boro])
                    boro = boro_options[borough_form] if borough_form < len(boro_options) else boro
                boro_list.append(boro)
            except Exception:
                continue
        if boro_list:
            borough_col = col_name.get("borough", "borough")
            boro_vals = ",".join(f"'{b}'" for b in sorted(set(boro_list)))
            return f"{borough_col} IN ({boro_vals})"
        return None

    if geo_unit == "BBL_SPLIT":
        boro_list, block_list, lot_list = [], [], []
        col_lot = col_digit.get("lot", "0002")
        adding_zero = True if len(col_lot) == 5 else False
        for full_bbl in bbls_to_use:
            try:
                s = str(full_bbl)
                boro = s[0]
                block = s[1:6]
                lot = s[6:]
                if borough_type.isalpha():
                    boro_options = BORO_CODE_MAP.get(boro, [boro])
                    boro = boro_options[borough_form] if borough_form < len(boro_options) else boro
                if adding_zero:
                    lot = lot.zfill(5)
                boro_list.append(boro)
                block_list.append(block)
                lot_list.append(lot)
            except Exception:
                continue
        if boro_list and block_list and lot_list:
            borough_col = col_name.get("borough", "borough")
            block_col = col_name.get("block", "block")
            lot_col = col_name.get("lot", "lot")
            boro_vals = ",".join(f"'{b}'" for b in sorted(set(boro_list)))
            block_vals = ",".join(f"'{b}'" for b in sorted(set(block_list)))
            lot_vals = ",".join(f"'{b}'" for b in sorted(set(lot_list)))
            return (
                f"{borough_col} IN ({boro_vals}) "
                f"AND {block_col} IN ({block_vals}) "
                f"AND {lot_col} IN ({lot_vals})"
            )
        return None

    logger.warning(f"Unknown geo_unit '{geo_unit}'; no where clause built.")
    return None


def _build_filter_for_dataset(
    ds,
    resolved_bbls: List[str],
    nearby_bbls: List[str],
    bundle_lookup: Optional[Dict[str, GeoBundle]] = None,
):
    ds_name = ds.name
    ds_conf = DATASET_CONFIG.get(ds_name, {})
    geo_unit = ds_conf.get("geo_unit", "BBL").upper()
    borough_type = ds_conf.get("Borough", "")
    borough_form = ds_conf.get("Borough_form", 0) 
    col_name = ds_conf.get("col_names", {}) 
    col_digit = ds_conf.get("cols", {})
    mode = ds_conf.get("mode", "street")
    want_surrounding = ds_conf.get("surrounding", False)

    try:
        bbls_to_use = nearby_bbls if want_surrounding else (resolved_bbls[:1] if resolved_bbls else [])
        where_str = _build_where_for_geo_unit(geo_unit, bbls_to_use, borough_type,borough_form,col_name,col_digit,bundle_lookup=bundle_lookup)
        if where_str:
            filter_def = {"where": where_str, "limit": 1000}
            logger.info(f"Applied filter on {ds_name} [{geo_unit}] ({mode}): {where_str}")
        else:
            filter_def = {"limit": 200}
            logger.warning(f"No valid filter for {ds_name}; using preview mode.")
    except NotImplementedError:
        logger.warning(f"Skipping dataset '{ds_name}' — API not implemented.")
        filter_def = {"limit": 0}
    except Exception as exc:
        logger.error(f"Unexpected error on {ds_name}: {exc}")
        filter_def = {"limit": 100}

    return ds_name, filter_def


def build_dataset_filters_for_handler(
    handler,
    resolved_bbls: List[str],
    nearby_bbls: List[str],
    bundle_lookup: Optional[Dict[str, GeoBundle]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Build the filter dict per dataset in handler using resolved and nearby BBLs per DATASET_CONFIG."""
    filters: Dict[str, Dict[str, Any]] = {}
    future_map = {
        ds.name: _GEO_EXECUTOR.submit(
            _build_filter_for_dataset,
            ds,
            resolved_bbls,
            nearby_bbls,
            bundle_lookup,
        )
        for ds in handler
    }

    for ds in handler:
        future = future_map.get(ds.name)
        if future is None:
            continue
        try:
            ds_name, filter_def = future.result()
        except Exception as exc:
            ds_name = ds.name
            logger.error(f"Dataset filter task failed for {ds_name}: {exc}")
            filter_def = {"limit": 100}
        filters[ds_name] = filter_def
    return filters

def get_dataset_filters(
    addresses: List[Dict[str, Any]],
    handler,
    surrounding: bool = True,
) -> Tuple[Dict[str, Dict[str, Any]], List[GeoBundle]]:
    """Return dataset filters along with the resolved GeoBundles for downstream reuse."""

    filters: Dict[str, Dict[str, Any]] = {}

    if not addresses:
        logger.warning("No address provided — using default preview mode.")
        for ds in handler:
            filters[ds.name] = {"limit": 200}
        return filters, []

    # Step 1: Resolve addresses to GeoBundles/BBLs
    resolution = resolve_geo_bundles_from_addresses(addresses)
    resolved_bbls = resolution.bbls

    if not resolved_bbls:
        logger.warning("No BBLs resolved from provided addresses; using preview mode.")
        for ds in handler:
            filters[ds.name] = {"limit": 200}
        return filters, resolution.bundles

    bundle_lookup = {bundle.bbl: bundle for bundle in resolution.bundles if bundle.bbl}

    # Step 2: Aggregate surrounding BBLs (optional)
    nearby_bbls = aggregate_surrounding_bbls(resolved_bbls, surrounding=surrounding)

    # Step 3: Build dataset filters
    filters = build_dataset_filters_for_handler(
        handler,
        resolved_bbls,
        nearby_bbls,
        bundle_lookup=bundle_lookup,
    )

    logger.info(
        "Final dataset filters returned for %d datasets using %d resolved BBLs (bundles=%d)",
        len(filters),
        len(resolved_bbls),
        len(resolution.bundles),
    )

    return filters, resolution.bundles