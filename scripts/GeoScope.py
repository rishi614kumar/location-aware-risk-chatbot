"""
GeoScope stub: returns a filter dict for each dataset.
Replace logic here to generate spatial filters based on addresses and handler.datasets.
"""
from __future__ import annotations
from typing import Dict, Any, List

from api.GeoClient import get_bbl_from_address
from adapters.surrounding import get_surrounding_bbls_from_bbl


def get_dataset_filters(addresses: List[Dict[str, Any]], handler) -> Dict[str, Dict[str, Any]]:
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

    # --- Step 3: build dataset filters ---
    bbl_list_str = ",".join(f"'{b}'" for b in nearby_bbls)
    for ds in handler:
        try:
            df = getattr(ds, "df", None)
            if df is None:
                df = getattr(ds, "_df_cache", None)

            if df is not None and hasattr(df, "columns"):
                df.columns = [c.upper() for c in df.columns]

            if df is not None and "BBL" in df.columns:
                bbl_list_str = ",".join(f"'{b}'" for b in nearby_bbls)
                where_str = f"BBL in ({bbl_list_str})"
                filters[ds.name] = {"where": where_str, "limit": 1000}
                print(f"✅ Applying spatial filter on {ds.name}: {len(nearby_bbls)} BBLs")
            else:
                filters[ds.name] = {"limit": 200}
                print(f"{ds.name} has no BBL column; using default limit.")
        except NotImplementedError:
            print(f"⚠️ Skipping dataset '{ds.name}' — API not implemented.")
            filters[ds.name] = {"limit": 0}
        except Exception as e:
            print(f"⚠️ Unexpected error on {ds.name}: {e}")
            filters[ds.name] = {"limit": 100}

    return filters