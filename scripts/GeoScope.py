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
from adapters.street_span import get_lion_span_from_bbl
from config.settings import DATASET_CONFIG

def _resolve_single_bbl(record: Dict[str, Any]) -> Optional[str]:
    house = (record.get("house_number") or "").strip()
    street = (record.get("street_name") or "").strip()
    borough = (record.get("borough") or "").strip()

    if not street or not borough:
        print(f"⚠️ No proper address for {normalized} ({borough}): {exc}")
        return None

    normalized = street.replace(" and ", " & ").replace(" AND ", " & ")

    if "&" in normalized:
        try:
            street_one, street_two = [s.strip() for s in normalized.split("&", 1)]
            bbl_intersection = get_bbl_from_intersection(street_one, street_two, borough)
            print(f"✅ Geoclient intersection lookup: {street_one} & {street_two}, {borough} → BBL {bbl_intersection}")
            return bbl_intersection
        except Exception as exc:
            print(f"⚠️ Intersection lookup failed for {normalized} ({borough}): {exc}")
            return None

    if house:
        try:
            bbl_address = get_bbl_from_address(f"{house} {street}", borough)
            print(f"✅ Geoclient lookup: {house} {street}, {borough} → BBL {bbl_address}")
            return bbl_address
        except Exception as exc:
            print(f"⚠️ Address lookup failed for {house} {street} ({borough}): {exc}")

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
                vals = get_lion_span_from_bbl(b)
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
            print(f"⚠️ Surrounding conversion failed for {b} ({geo_unit}): {e}")
            continue

    return list(units)

def get_dataset_filters(addresses: List[Dict[str, Any]], handler, surrounding=True) -> Dict[str, Dict[str, Any]]:
    filters: Dict[str, Dict[str, Any]] = {}

    if not addresses:
        print("⚠️ No address provided — using default preview mode.")
        for ds in handler:
            filters[ds.name] = {"limit": 200}
        return filters

    # --- Step 1: resolve each address → BBL ---
    resolved_bbls: List[str] = []
    for record in addresses:
        bbl = _resolve_single_bbl(record)
        if bbl:
            resolved_bbls.append(bbl)

    resolved_bbls = list(dict.fromkeys(bbl for bbl in resolved_bbls if bbl))

    if not resolved_bbls:
        print("⚠️ No BBLs resolved from provided addresses; using preview mode.")
        for ds in handler:
            filters[ds.name] = {"limit": 200}
        return filters

    # --- Step 2: find surrounding BBLs ---
    if surrounding:
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
                print(f"⚠️ Surrounding lookup failed for {bbl}: {exc}")
                nearby_set.add(bbl)
        
        nearby_bbls = sorted(nearby_set)
        print(f"✅ Aggregated {len(nearby_bbls)} unique BBLs from {len(resolved_bbls)} addresses")
    else:
        nearby_bbls = [bbl]

    # --- Step 3: build dataset filters ---
    for ds in handler:
        ds_name = ds.name
        ds_conf = DATASET_CONFIG.get(ds_name, {})
        geo_unit = ds_conf.get("geo_unit", "BBL").upper()
        mode = ds_conf.get("mode", "street")
        want_surrounding = ds_conf.get("surrounding", False)

        where_str = None

        try:
            if not want_surrounding:
                bbls_to_use = [bbl]
            else:
                bbls_to_use = nearby_bbls

            filter_ids = get_surrounding_units(bbls_to_use, geo_unit)
            if geo_unit == "BBL":
                vals = ",".join(f"'{b}'" for b in filter_ids)
                where_str = f"BBL IN ({vals})"

            elif geo_unit == "PRECINCT":
                vals = ",".join(f"'{p}'" for p in filter_ids)
                where_str = f"Precinct IN ({vals})"

            elif geo_unit.startswith("NTA"):
                vals = ",".join(f"'{n}'" for n in filter_ids)
                where_str = f"NTA IN ({vals})"

            elif geo_unit == "STREETSPAN":
                vals = ",".join(f"'{s}'" for s in filter_ids)
                where_str = f"SegmentID IN ({vals})"

            elif geo_unit in ("LONLAT", "COORD"):
                coords = [lonlat.split(',') for lonlat in filter_ids]
                conds = " OR ".join([f"(Longitude={lon} AND Latitude={lat})" for lon, lat in coords])
                where_str = conds

            elif geo_unit == "BBL_SPLIT":
                boro_list, block_list, lot_list = [], [], []
                for full_bbl in nearby_bbls:
                    try:
                        s = str(full_bbl)
                        boro = s[0]
                        block = s[1:6]
                        lot = s[6:]
                        boro_list.append(boro)
                        block_list.append(block)
                        lot_list.append(lot)
                    except Exception:
                        continue

                if boro_list and block_list and lot_list:
                    boro_vals = ",".join(f"'{b}'" for b in sorted(set(boro_list)))
                    block_vals = ",".join(f"'{b}'" for b in sorted(set(block_list)))
                    lot_vals = ",".join(f"'{b}'" for b in sorted(set(lot_list)))
                    where_str = (
                        f"Borough IN ({boro_vals}) "
                        f"AND Block IN ({block_vals}) "
                        f"AND Lot IN ({lot_vals})"
                    )
                    print(f"✅ Applied split-BBL filter on {ds_name}: {len(block_list)} lots")

            else:
                print(f"⚠️ Unknown geo_unit '{geo_unit}' for {ds_name}; fallback to preview.")

            # Finalize filter
            if where_str:
                filters[ds_name] = {"where": where_str, "limit": 1000}
                print(f"✅ Applied filter on {ds_name} [{geo_unit}] ({mode}): {where_str}")
            else:
                filters[ds_name] = {"limit": 200}
                print(f"⚠️ No valid filter for {ds_name}; using preview mode.")

        except NotImplementedError:
            print(f"⚠️ Skipping dataset '{ds_name}' — API not implemented.")
            filters[ds_name] = {"limit": 0}
        except Exception as e:
            print(f"⚠️ Unexpected error on {ds_name}: {e}")
            filters[ds_name] = {"limit": 100}

    return filters