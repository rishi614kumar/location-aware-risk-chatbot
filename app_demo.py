"""
Demo script for using Geoclient API helper functions.
Run with:  python -m app_demo
"""

from api.geoclient import Geoclient, get_bbl_from_address, get_bins_from_address, get_bin_from_address,get_bins_from_bbl, get_bbl_from_bin
from scripts.GeoBundle import geo_from_address
from adapters.precinct import get_precinct_from_bbl, get_bbls_from_precinct # Rishi
from adapters.nta import get_nta_from_bbl, get_bbls_from_nta  # Sharon
from adapters.street_span import get_bbls_from_lion_span, get_lion_span_from_bbl  # Kevin
from adapters.epsg import get_lonlat_to_stateplane, get_stateplane_to_lonlat  # Max
from adapters.coords import get_bbl_from_lonlat, get_bbls_near_lonlat, get_lonlat_from_bbl  # Louis
from scripts.DataHandler import DataHandler
from llm.LLMParser import get_default_parser
from llm.LLMInterface import make_backend
from IPython.display import display
import json
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
print("\n------------------------Example 5: Address to all BINs (via BBL)------------------------")
bbl_for_address = get_bbl_from_address("237 Park Ave", "Manhattan")
bins = get_bins_from_bbl(bbl_for_address) if bbl_for_address else []
print("All BINs (via BBL):", bins)

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
print("Example: NTA for BBL", bbl)
nta_code = get_nta_from_bbl(bbl)
print("NTA code for BBL:", nta_code)
if nta_code:
    bbls_in_nta = get_bbls_from_nta(nta_code)
    print(f"BBLs in NTA {nta_code}:", len(bbls_in_nta))
    print(f"First 10 BBLs in NTA {nta_code}:", bbls_in_nta[:10])  # preview first 10
else:
    print("No NTA found for this BBL.")


# ---------------- Kevin: LION Demo ----------------
print("\n---------------- Example 10: LION Street Segment Lookups ----------------")

# Example 1: From street -> BBLs
bbls = get_bbls_from_lion_span("EAST 168 STREET")
print(f"Found {len(bbls)} BBLs\n")

# Example 2: From BBL -> Street(s)
lion_bbl = "2023720020"
street = get_lion_span_from_bbl(bbl="2023720020")
print(f"BBL {lion_bbl} corresponds to: {street}")

# ---------------- Max: EPSG Demo ----------------
print("\n---------------- Example 11: EPSG / Coordinate Conversion ----------------")
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

print("\n------------------------Example 13: BBL <-> Precinct ------------------------")
precinct=18
bbls = get_bbls_from_precinct(precinct)
print(f"Precinct {precinct} -> {len(bbls)} BBLs")
preview = bbls[:20]
print("First 20 BBLs:", preview)

# Showcase: Get precinct from a sample BBL
sample_bbl = "1013007501"
precinct_for_bbl = get_precinct_from_bbl(sample_bbl)
print(f"Precinct for BBL {sample_bbl}:", precinct_for_bbl)

"""
NOTES:
- you must have GEOCLIENT_API_KEY/ GEOCLIENT_API_KEY in your environment (.env or settings)
- borough names must be spelled exactly: 'Manhattan', 'Bronx', 'Brooklyn', 'Queens', 'Staten Island'
- raw outputs are simplified using the _normalize() helper
"""
print('\n------------------------ LLM Examples ------------------------')
print("\n------------------------ Example 1: Environmental & Health Risks ------------------------")
example_query = "Are there asbestos filings or air quality complaints near 45-10 21st Street in Queens?"
llm_backend = make_backend(provider="gemini")
parser = get_default_parser(backend=llm_backend)
result = parser.route_query_to_datasets(example_query)
print("\nQuery:", example_query)
print("Router Result:", json.dumps(result, indent=2))

handler = DataHandler(result["dataset_names"])
first_dataset = getattr(handler, "d1")
if first_dataset:
    print("\nFirst Dataset:", first_dataset.name)
    print("Description:", first_dataset.description, '\n')
    print(first_dataset.df.shape)
    display(first_dataset.df.head())
    try:
        df = first_dataset.df
        print(f"✅ Columns ({len(df.columns)}):\n{df.columns.tolist()}\n")
    except Exception as e:
        print(f"⚠️ Failed to load DataFrame: {e}")

second_dataset = getattr(handler, 'd2')
if second_dataset:
    print("\nSecond Dataset:", second_dataset.name)
    print("Description:", second_dataset.description, '\n')
    print(second_dataset.df.shape)
    display(second_dataset.df.head())

print("\n------------------------ Example 2: Comparative Site Queries ------------------------")
example_query = 'Which location has fewer open permits: Jamaica Avenue in Queens or Broadway in Upper Manhattan?”'
result = parser.route_query_to_datasets(example_query)
print("\nQuery:", example_query)
print("Router Result:", json.dumps(result, indent=2))

handler = DataHandler(result["dataset_names"])
first_dataset = getattr(handler, "d1")
if first_dataset:
    print("\nFirst Dataset:", first_dataset.name)
    print("Description:", first_dataset.description)

second_dataset = getattr(handler, 'd2')
if second_dataset:
    print("\nSecond Dataset:", second_dataset.name)
    print("Description:", second_dataset.description)

print("\n------------------------ Example 3 Public Safety & Social Context: No Specific Address ------------------------")
example_query = 'How does population density compare between Jackson Heights and Downtown Brooklyn?'
result = parser.route_query_to_datasets(example_query)
print("\nQuery:", example_query)
print("Router Result:", json.dumps(result, indent=2))

handler = DataHandler(result["dataset_names"])
first_dataset = getattr(handler, "d1")
if first_dataset:
    print("\nFirst Dataset:", first_dataset.name)
    print("Description:", first_dataset.description)

second_dataset = getattr(handler, 'd2')
if second_dataset:
    print("\nSecond Dataset:", second_dataset.name)
    print("Description:", second_dataset.description)
