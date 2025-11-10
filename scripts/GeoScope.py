"""
GeoScope stub: returns a filter dict for each dataset.
Replace logic here to generate spatial filters based on addresses and handler.datasets.
"""
from __future__ import annotations
from typing import Dict, Any, List

from api.GeoClient import get_bbl_from_address
from adapters.surrounding import get_surrounding_bbls_from_bbl
from adapters.coords import get_lonlat_from_bbl
from adapters.precinct import get_precinct_from_bbl
from adapters.nta import get_nta_from_bbl
from adapters.street_span import get_lion_span_from_bbl
from config.settings import DATASET_CONFIG


def get_dataset_filters(addresses: List[Dict[str, Any]], handler, surrounding=True) -> Dict[str, Dict[str, Any]]:
    filters: Dict[str, Dict[str, Any]] = {}

    if not addresses:
        print("⚠️ No address provided — using default preview mode.")
        for ds in handler:
            filters[ds.name] = {"limit": 200}
        return filters

    # --- Step 1: resolve address → BBL ---
    first = addresses[0]
    house = first.get("house_number")
    street = first.get("street_name")
    borough = first.get("borough")

    if not (house and street and borough):
        print("⚠️ Incomplete address info; skipping geoclient lookup.")
        for ds in handler:
            filters[ds.name] = {"limit": 200}
        return filters

    try:
        bbl = get_bbl_from_address(f"{house} {street}", borough)
        print(f"✅ Geoclient lookup: {house} {street}, {borough} → BBL {bbl}")
    except Exception as e:
        print(f"⚠️ Geoclient lookup failed: {e}")
        bbl = None

    if not bbl:
        print("⚠️ No BBL found; fallback to preview.")
        for ds in handler:
            filters[ds.name] = {"limit": 200}
        return filters

    # --- Step 2: find surrounding BBLs ---
    if surrounding:
        try:
            nearby_bbls = list(get_surrounding_bbls_from_bbl(
                bbl=bbl,
                mode="street",      # or "radius"
                include_self=True
            ))
            print(f"✅ Found {len(nearby_bbls)} surrounding BBLs")
        except Exception as e:
            print(f"⚠️ Surrounding lookup failed: {e}")
            nearby_bbls = [bbl]
    else:
        nearby_bbls = [bbl]

    # --- Step 3: build dataset filters ---
    for ds in handler:
        ds_name = ds.name
        ds_conf = DATASET_CONFIG.get(ds_name, {})
        geo_unit = ds_conf.get("geo_unit", "BBL").upper()
        mode = ds_conf.get("mode", "street")

        where_str = None

        try:
            if geo_unit == "BBL":
                bbl_vals = ",".join(f"'{b}'" for b in nearby_bbls)
                where_str = f"BBL IN ({bbl_vals})"

            elif geo_unit == "PRECINCT":
                precinct = get_precinct_from_bbl(bbl)
                if precinct:
                    where_str = f"Precinct = '{precinct}'"

            elif geo_unit[:3] == "NTA":
                nta = get_nta_from_bbl(bbl)
                if nta:
                    where_str = f"{geo_unit} = '{nta}'"

            elif geo_unit == "STREETSPAN":
                span_ids = get_lion_span_from_bbl(bbl)
                if span_ids:
                    span_vals = ",".join(f"'{s}'" for s in span_ids)
                    where_str = f"SegmentID IN ({span_vals})"

            elif geo_unit in ("LONLAT", "COORD"):
                lon, lat = get_lonlat_from_bbl(bbl)
                where_str = f"Longitude = {lon} AND Latitude = {lat}"

            elif geo_unit == "BBL_SPLIT":
                boro_list, block_list, lot_list = [], [], []
                for full_bbl in nearby_bbls:
                    try:
                        s = str(full_bbl)
                        boro = int(s[0])
                        block = int(s[1:6])
                        lot = int(s[6:])
                        boro_list.append(boro)
                        block_list.append(block)
                        lot_list.append(lot)
                    except Exception:
                        continue

                if boro_list and block_list and lot_list:
                    boro_vals = ",".join(map(str, sorted(set(boro_list))))
                    block_vals = ",".join(map(str, sorted(set(block_list))))
                    lot_vals = ",".join(map(str, sorted(set(lot_list))))
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
