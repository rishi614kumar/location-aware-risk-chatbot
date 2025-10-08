"""
Demo script for using Geoclient API helper functions.
Run with:  python -m app_demo
"""

from api.geoclient import Geoclient, get_bbl_from_address, get_bins_from_address, get_bin_from_address,get_bins_from_bbl, get_bbl_from_bin
from scripts.geobundle import geo_from_address
from adapters.precinct import get_precinct_from_bbl, get_bbls_from_precinct # Rishi
from adapters.nta import get_nta_from_bbl  # Sharon
from adapters.lion import get_bbls_from_lion_span  # Kevin
from adapters.epsg import get_lonlat_to_stateplane, get_stateplane_to_lonlat  # Max
from adapters.coords import get_bbl_from_lonlat, get_bbls_near_lonlat, get_lonlat_from_bbl  # Louis
# Initialize the client
gc = Geoclient()

# Address -> Full Info 
print("\n------------------------Example 1: Get info from address------------------------")
info = gc.address("237", "Park Ave", "Manhattan")
print("Normalized Output:", info)
print("BBL:", info.get("bbl"))
print("BIN:", info.get("bin"))
print("NTA:", info.get("nta"))
print("Precinct:", info.get("policePrecinct"))
print("Borough:", info.get("borough"))
print("Coordinates:", (info.get("latitude"), info.get("longitude")))

# Simple Address String Shortcut 
print("\n------------------------Example 2: Using get_bbl_from_address helper------------------------")
bbl = get_bbl_from_address("237 Park Ave", "Manhattan")
print("BBL:", bbl)


# Error Handling Example 
print("\n------------------------Example 3: Invalid BBL handling------------------------")
try:
    gc.bbl("9999999999")  # invalid BBL
    print("OK.")
except Exception as e:
    print("expected error:", e)


from api.geoclient import (
    get_bins_from_address, get_bin_from_address,
    get_bins_from_bbl, get_bbl_from_bin
)

# Address -> one BIN
print("\n------------------------Example 4: Address to one BIN------------------------")
bin_primary = get_bin_from_address("237 Park Ave", "Manhattan")
print("Primary BIN:", bin_primary)

# Address -> all BINs 
print("\n------------------------Example 5: Address to all BINs------------------------")
bins = get_bins_from_address("237 Park Ave", "Manhattan")
print("All BINs:", bins)

# BBL -> all BINs on that lot
print("\n------------------------Example 6: BBL to all BINS------------------------")
bins_for_bbl = get_bins_from_bbl("1013007501")
print("BINs for BBL 1013007501:", bins_for_bbl)

# BIN -> BBL
print("\n------------------------Example 7: BIN to BBL------------------------")
bbl_from_bin = get_bbl_from_bin(bin_primary) if bin_primary else None
print("BBL for primary BIN:", bbl_from_bin)

print("\n------------------------Example 8: Geo bundle from address------------------------")
bundle = geo_from_address("237 Park Ave", "Manhattan")
print(bundle)
print(type(bundle)) 
print("Precinct:", bundle.precinct)

 # ---------------- Sharon: NTA Demo ----------------
print("\n---------------- Example 9: NTA Lookups ----------------")
print("(TODO: Sharon) Example: NTA for BBL", bbl)
# Example expected usage:
# nta = get_nta_from_bbl(bbl)
# print("NTA:", nta)

# ---------------- Kevin: LION Demo ----------------
print("\n---------------- Example 10: LION Street Segment Lookups ----------------")
print("(TODO: Kevin) Example: Find all BBLs for a LION street span")
# Example expected usage:
# lion_id = "LION1234"
# bbls = get_bbls_from_lion_segment(lion_id)
# print("BBLs for segment:", bbls)

# ---------------- Max: EPSG Demo ----------------
print("\n---------------- Example 11: EPSG / Coordinate Conversion ----------------")
print("(TODO: Max) Example: Convert WGS84 lat/lon to State Plane coordinates")
lat, lon = 40.7539, -73.9755

# Convert WGS84 (lon, lat) -> NY State Plane (EPSG:2263, feet)
state_x, state_y = get_lonlat_to_stateplane(lon, lat)  # function expects (lon, lat)
print(f"WGS84 -> StatePlane(2263): ({lon:.6f}, {lat:.6f}) -> (x={state_x:.3f}, y={state_y:.3f})")

# Convert back: State Plane -> WGS84 (lon, lat)
lon2, lat2 = get_stateplane_to_lonlat(state_x, state_y)
print(f"StatePlane(2263) -> WGS84: (x={state_x:.3f}, y={state_y:.3f}) -> ({lon2:.6f}, {lat2:.6f})")

# Quick sanity check (tolerance ~ few feet)
d_lat = abs(lat - lat2)
d_lon = abs(lon - lon2)
print(f"Round-trip error: d_lat={d_lat:.8f}, d_lon={d_lon:.8f}")

# ---------------- Louis: Coordinates Demo ----------------
print("\n---------------- Example 12: Coordinate <-> BBL ----------------")
lon, lat = -73.9755, 40.7539
print("Coordinate:", (lon, lat))
# BBL from single coordinate
result = get_bbl_from_lonlat(lon, lat)
print("Nearest BBL and distance (ft):", result)

# Nearby BBLs within 50 ft
nearby = get_bbls_near_lonlat(lon, lat, buffer_ft=50)
print("Nearby BBLs (within 50 ft):", nearby)

# Get coordinate back from a sample BBL
if result:
    bbl = result[0]
    coords = get_lonlat_from_bbl(bbl)
    print(f"Representative coordinate for BBL {bbl}:", coords)
else:
    print("No nearby BBL found.")

print("\n------------------------Example 13: Get all BBLs for a given Precinct (PLUTO)------------------------")
precinct=18
bbls = get_bbls_from_precinct(precinct)
print(f"Precinct {precinct} -> {len(bbls)} BBLs")
preview = bbls[:20]
print("First 20 BBLs:", preview)

"""
NOTES:
- you must have GEOCLIENT_API_KEY in your environment (.env or settings)
- borough names must be spelled exactly: 'Manhattan', 'Bronx', 'Brooklyn', 'Queens', 'Staten Island'
- raw outputs are simplified using the _normalize() helper
"""