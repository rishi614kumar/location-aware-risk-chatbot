"""
GeoScope stub: returns a filter dict for each dataset.
Replace logic here to generate spatial filters based on addresses and handler.datasets.
"""
from __future__ import annotations
from typing import Dict, Any, List, Optional
from itertools import combinations
from api.GeoClient import get_bbl_from_address, get_bbl_from_intersection
from adapters.surrounding import get_surrounding_bbls_from_bbl
from adapters.surrounding import get_surrounding_bbls_from_bbl
from adapters.coords import get_lonlat_from_bbl
from adapters.precinct import get_precinct_from_bbl
from adapters.nta import get_nta_from_bbl
from adapters.street_span import get_lion_span_from_bbl, get_segment_id_from_bbl
from config.settings import DATASET_CONFIG, BORO_CODE_MAP
from config.logger import logger

def _resolve_single_bbl(record: Dict[str, Any]) -> Optional[str]:
    house = (record.get("house_number") or "").strip()
    street = (record.get("street_name") or "").strip()
    borough = (record.get("borough") or "").strip()

    if not street or not borough:
        logger.warning(f"No proper address for {normalized} ({borough})")
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


def get_surrounding_units(bbl_list: List[str], geo_unit: str) -> List[str]:
    """
    Given a list of nearby BBLs, convert all to the specified geo_unit and
    deduplicate results.
    """
    units = set()

    for b in bbl_list:
        try:
            if geo_unit == "PRECINCT":
                val = get_precinct_from_bbl(b)
            elif geo_unit.startswith("NTA"):
                val = get_nta_from_bbl(b)
            elif geo_unit == "STREETSPAN":
                vals = get_segment_id_from_bbl(b)
                if vals:
                    units.update(vals)
                continue
            elif geo_unit in ("LONLAT", "COORD"):
                lon, lat = get_lonlat_from_bbl(b)
                val = f"{lon},{lat}"
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
    resolved_bbls: List[str] = []
    for record in addresses or []:
        bbl = _resolve_single_bbl(record)
        if bbl:
            resolved_bbls.append(bbl)
    # de-duplicate while preserving order
    return list(dict.fromkeys(b for b in resolved_bbls if b))


def aggregate_surrounding_bbls(resolved_bbls: List[str], surrounding: bool = True) -> List[str]:
    """Aggregate surrounding BBLs for each resolved BBL when surrounding=True; otherwise return resolved list."""
    if not resolved_bbls:
        return []
    if not surrounding:
        return list(dict.fromkeys(resolved_bbls))

    nearby_set = set()
    for bbl in resolved_bbls:
        try:
            results = get_surrounding_bbls_from_bbl(
                bbl=bbl,
                mode="street",
                include_self=True,
            )
            for item in results:
                nearby_set.add(item)
        except Exception as exc:
            logger.warning(f"Surrounding lookup failed for {bbl}: {exc}")
            nearby_set.add(bbl)
    nearby_bbls = sorted(nearby_set)
    logger.info(f"Aggregated {len(nearby_bbls)} unique BBLs from {len(resolved_bbls)} addresses")
    return nearby_bbls


def _build_where_for_geo_unit(geo_unit: str, bbls_to_use: List[str], borough_type: str, borough_form: int,col_name: Dict, col_digit: Dict) -> Optional[str]:
    """Build a Socrata where clause for a specific geo unit given a list of BBLs to use."""
    geo_unit = (geo_unit or "BBL").upper()
    if not bbls_to_use:
        return None

    if geo_unit == "BBL":
        vals = ",".join(f"'{b}'" for b in bbls_to_use)
        return f"BBL IN ({vals})"

    if geo_unit == "PRECINCT":
        ids = get_surrounding_units(bbls_to_use, geo_unit)
        vals = ",".join(f"'{p}'" for p in ids)
        return f"PCT in [{vals}]" if vals else None

    if geo_unit.startswith("NTA"):
        ids = get_surrounding_units(bbls_to_use, geo_unit)
        vals = ",".join(f"'{n}'" for n in ids)
        return f"nta2020 IN ({vals})" if vals else None

    if geo_unit == "STREETSPAN":
        ids = get_surrounding_units(bbls_to_use, geo_unit)
        vals = ",".join(f"'{s}'" for s in ids)
        return f"segmentid IN ({vals})" if vals else None

    if geo_unit in ("LONLAT", "COORD"):
        ids = get_surrounding_units(bbls_to_use, geo_unit)
        coords = [tuple(map(float, lonlat.split(','))) for lonlat in ids]
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


def build_dataset_filters_for_handler(handler, resolved_bbls: List[str], nearby_bbls: List[str]) -> Dict[str, Dict[str, Any]]:
    """Build the filter dict per dataset in handler using resolved and nearby BBLs per DATASET_CONFIG."""
    filters: Dict[str, Dict[str, Any]] = {}
    for ds in handler:
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
            where_str = _build_where_for_geo_unit(geo_unit, bbls_to_use, borough_type, borough_form,col_name,col_digit)
            if where_str:
                filters[ds_name] = {"where": where_str, "limit": 1000}
                logger.info(f"Applied filter on {ds_name} [{geo_unit}] ({mode}): {where_str}")
            else:
                filters[ds_name] = {"limit": 200}
                logger.warning(f"No valid filter for {ds_name}; using preview mode.")
        except NotImplementedError:
            logger.warning(f"Skipping dataset '{ds_name}' — API not implemented.")
            filters[ds_name] = {"limit": 0}
        except Exception as e:
            logger.error(f"Unexpected error on {ds_name}: {e}")
            filters[ds_name] = {"limit": 100}
    return filters

def get_dataset_filters(addresses: List[Dict[str, Any]], handler, surrounding=True) -> Dict[str, Dict[str, Any]]:
    """Orchestrator that delegates to modular helpers for clarity and testability."""
    filters: Dict[str, Dict[str, Any]] = {}

    if not addresses:
        logger.warning("No address provided — using default preview mode.")
        for ds in handler:
            filters[ds.name] = {"limit": 200}
        return filters

    # Step 1: Resolve addresses to BBLs
    resolved_bbls = resolve_bbls_from_addresses(addresses)

    if not resolved_bbls:
        logger.warning("No BBLs resolved from provided addresses; using preview mode.")
        for ds in handler:
            filters[ds.name] = {"limit": 200}
        return filters

    # Step 2: Aggregate surrounding BBLs (optional)
    nearby_bbls = aggregate_surrounding_bbls(resolved_bbls, surrounding=surrounding)

    # Step 3: Build dataset filters
    filters = build_dataset_filters_for_handler(handler, resolved_bbls, nearby_bbls)

    # Log the final filters dict for debugging
    logger.info(f"Final dataset filters returned: {filters}")

    return filters