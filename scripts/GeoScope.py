"""
GeoScope stub: returns a filter dict for each dataset.
Replace logic here to generate spatial filters based on addresses and handler.datasets.
"""
from __future__ import annotations
from typing import Dict, Any, List, Optional
from itertools import combinations
from api.GeoClient import get_bbl_from_address, get_bbl_from_intersection
from adapters.surrounding import get_surrounding_bbls_from_bbl


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

def get_dataset_filters(addresses: List[Dict[str, Any]], handler) -> Dict[str, Dict[str, Any]]:
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

    # --- Step 2: find surrounding BBLs for each resolved BBL ---
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
