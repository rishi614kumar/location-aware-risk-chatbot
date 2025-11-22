from dotenv import load_dotenv
import os

load_dotenv()

CHATBOT_TYPEWRITER_DELAY = 0.01  # seconds per character; adjust for speed (set to 0 for fastest)

GEOCLIENT_API_KEY= os.getenv("GEOCLIENT_API_KEY")
MAPPLUTO_GDB_PATH = os.getenv("MAPPLUTO_GDB_PATH")
LION_GDB_PATH = os.getenv("LION_GDB_PATH")
NTA_PATH = os.getenv("NTA_PATH") # https://data.cityofnewyork.us/resource/9nt8-h7nd.geojson
CRIME_PATH = os.getenv("CRIME_PATH")

FLATFILE_PATHS = {
    "Crime": CRIME_PATH,
    "Sewer System Data": os.getenv("SEWER_SYSTEM_DATA_PATH"),
    # Add more flatfile dataset paths here
}
# For gdb datasets, specify layer names
FLATFILE_LAYERS = {
    "Crime": None,  # e.g., "layer_name"
    "Sewer System Data": "MS4DRAINAGEAREAS"
}



# STREET_SPAN SETTINGS
MAX_BUFFER_FT = 120  # Maximum buffer distance in feet
MIN_BUFFER_FT = 10   # Minimum buffer distance in feet
DEFAULT_BUFFER_INCREMENT_FT = 10  # Default increment to add to street width
DEFAULT_BUFFER_FT = 30  # Default buffer distance when street width is unknown
# If we don’t know the street width, assume DEFAULT_BUFFER_FT ft. Otherwise, take the width plus DEFAULT_BUFFER_INCREMENT_FT ft of margin, but keep it between MIN_BUFFER_FT and MAX_BUFFER_FT ft total.

# Human-readable context for each dataset; extend as new sources come online.
DATASET_DESCRIPTIONS = {
    "NYC OpenData Zoning and Tax Lot Database": (
        "Contains zoning and land use classifications for each tax lot, "
        "used to determine regulatory and development constraints."
    ),
    "NYC OpenData Automated Traffic Volume Counts": (
        "Records traffic volume at bridges and major roads, supporting transportation "
        "and congestion analysis for various locations across the city."
    ),
    "NYC OpenData Motor Vehicle Collisions": (
        "Documents traffic incidents and crash locations, helping identify "
        "transportation and public safety risk areas."
    ),
    "DOB Permits": (
        "Permits for construction and demolition activities in the City of New York. "
        "Each record represents the life cycle of one permit for one work type."
    ),
    "Asbestos Control Program": (
        "Any time asbestos abatement is performed on quantities greater than a minor project amount, "
        "the applicant has to file this form with DEP Asbestos Control Program (ACP)."
    ),
    "Digital City Map Shapefile": (
        "Represents the official street map of New York City, showing street lines and "
        "related geographic features, used for accurate base mapping."
    ),
    "Historic Districts map": (
        "Defines the boundaries of designated historic districts throughout NYC, "
        "as established by the Landmarks Preservation Commission."
    ),
    "Zoning GIS data": (
        "Includes zoning districts, special purpose districts, subdistricts, limited height districts, "
        "commercial overlays, and zoning map amendments for citywide land use analysis."
    ),
    "Population by Community Districts": (
        "Aggregated population counts by community district for decennial census years "
        "1970 through 2010, used for demographic and spatial trend analysis."
    ),
    "Population by Neighborhood Tabulation Area": (
        "Aggregations of census tracts grouped into Neighborhood Tabulation Areas (NTAs), "
        "providing demographic information and population change from 2000 to 2010."
    ),
    "Crime": (
        "Provides a statistical breakdown of reported crimes by citywide, borough, and precinct levels, "
        "used for public safety and social context analysis."
    ),
    "Street Construction Permits": (
        "Lists over 150 types of sidewalk and roadway construction permits issued to utilities, "
        "contractors, agencies, and homeowners for work on public streets."
    ),
    "MTA subway and other underground train lines": (
        "Contains spatial data for all NYC subway and Staten Island Railway stations and lines, "
        "used for accessibility and transportation network analysis."
    ),
    "City Owned and Leased Property": (
        "Identifies city-owned and leased properties, including location, agency, and use type, "
        "supporting land management and zoning analysis."
    ),
    "Sewer System Data": (
        "Represents the city’s stormwater and sewer infrastructure network, including "
        "MS4 outfalls, drainage areas, and connections to wastewater treatment facilities."
    ),
    "Clean Air Tracking System (CATS)": (
        "An online platform managed by the NYC Department of Environmental Protection "
        "that supports air quality–related registrations and permits for boilers, generators, "
        "gas stations, and industrial facilities."
    ),
    "Water and Sewer Permits": (
        "Contains information about applications approved and permits issued for water "
        "and sewer connections across the city."
    ),
    "DOB NOW: Build - Job Application Findings": (
        "Part of the DOB NOW online system that records job filings and applications "
        "for construction and alteration projects, excluding electrical and elevator work."
    ),
    "Citywide Catch Basins": (
        "NYCDEP Citywide Catch Basins. Catch basins are an important part of New York City’s "
        "7,500-mile sewer network. They collect stormwater and direct it to wastewater "
        "treatment facilities or nearby waterbodies. DEP maintains over 150,000 basins citywide."
    ),
    "Parks Monuments": (
        "Catalog of monuments maintained by NYC Parks, detailing their locations and associated "
        "boroughs for cultural and heritage mapping."
    ),
    "Citywide Hydrants": (
        "Lists all fire hydrant locations across NYC, used for public safety, emergency response, "
        "and infrastructure analysis."
    ),
    "Street Pavement Rating": (
        "The Citywide Catch Basins dataset maps over 150,000 catch basins that collect stormwater "
        "and direct it into NYC’s sewer network or nearby waterbodies, supporting drainage "
        "and flood management."
    ),
}

# Lightweight topical tags that downstream UIs can group/filter on.
DATASET_TAGS = {
    "Asbestos Control Program": ["asbestos", "environmental", "health"],
}

# Capabilities advertised by the dataset fetch layer.
DEFAULT_DATASET_FLAGS = dict(
    supports_point_radius=True,
    supports_intersections=True,
    supports_addresses=True,
)

# Socrata dataset identifiers (4-4 codes) when available.
DATASET_API_IDS = {
    "NYC OpenData Zoning and Tax Lot Database" : "fdkv-4t4z",
    "NYC OpenData Automated Traffic Volume Counts" : "7ym2-wayt",
    "NYC OpenData Motor Vehicle Collisions" : "h9gi-nx95",
    "DOB Permits" : "ipu4-2q9a",
    "Asbestos Control Program" : "vq35-j9qm",
    "Digital City Map Shapefile" : "y23c-72fa",
    "Historic Districts map" : "skyk-mpzq",
    "Zoning GIS data" : None,
    "Population by Community Districts" : "xi7c-iiu2",
    "Population by Neighborhood Tabulation Area" : "9nt8-h7nd",
    "Crime" : None,
    "Street Construction Permits" : "tqtj-sjs8",
    "MTA subway and other underground train lines" : "39hk-dx4f",
    "City Owned and Leased Property" : "fn4k-qyk2",
    "Sewer System Data" : None,
    "Clean Air Tracking System (CATS)" : "f4rp-2kvy",
    "Water and Sewer Permits" : "hphy-6g7m",
    "DOB NOW: Build - Job Application Findings" : "w9ak-ipjd",
    "Citywide Catch Basins" : "2w2g-fk3i",
    "Parks Monuments" : "6rrm-vxj9",
    "Citywide Hydrants" : "5bgh-vtsn",
    "Street Pavement Rating" : "6yyb-pb25"
}

# Fewshots for training LLM to handle category of queries
FEWSHOTS_MULTI = [
    ("Where are the top traffic accident hotspots within 500 feet of 163rd Street?",
     {"categories": ["Transportation & Traffic"], "datasets": ["NYC OpenData Motor Vehicle Collisions", "NYC OpenData Automated Traffic Volume Counts"], "confidence": 0.85, "borough": "Bronx"}),
    ("Any active DOB permits near 10 Jay St?",
     {"categories": ["Construction & Permitting"], "datasets": ["DOB permits", "Street Construction Permits"], "confidence": 0.85, "borough": "Brooklyn"}),
    ("Is this parcel in a historic district and what’s the zoning?",
     {"categories": ["Zoning & Land Use"], "datasets": ["Historic Districts map", "NYC OpenData Zoning and Tax Lot Database"], "confidence": 0.80, "borough": "Manhattan"}),
    ("Any flood or sewer risk around 123 Main St?",
     {"categories": ["Environmental & Health Risks"], "datasets": ["Sewer System Data", "Clean Air Tracking System (CATS)"], "confidence": 0.80, "borough": "Queens"}),
    ("Where are the nearest fire hydrants near Borough Hall?",
     {"categories": ["Public Safety & Social Context"], "datasets": ["Citywide Hydrants", "NYC OpenData Motor Vehicle Collisions"], "confidence": 0.75, "borough": "Brooklyn"}),
    ("Compare zoning and environmental risks for 149th Street & Grand Concourse versus 181st Street & St. Nicholas Avenue.",
     {"categories": ["Comparative Site Queries", "Zoning & Land Use", "Environmental & Health Risks"], "datasets": ["NYC OpenData Zoning and Tax Lot Database", "Sewer System Data"], "confidence": 0.88, "borough": "Manhattan"}),
]

# Fewshots for LLM to handle addresses and intersections
FEWSHOTS_ADDR = [
    ("Any active DOB permits near 10 Jay St?",
     {"addresses": [{"house_number": "10", "street_name": "Jay St", "borough": "Brooklyn", "raw": "10 Jay St", "notes": ""}]}),
    ("Compare zoning and environmental risks for 149th Street & Grand Concourse versus 181st Street & St. Nicholas Avenue.",
     {"addresses": [
         {"house_number": "", "street_name": "149th Street & Grand Concourse", "borough": "Bronx", "raw": "149th Street & Grand Concourse", "notes": ""},
         {"house_number": "", "street_name": "181st Street & St. Nicholas Avenue", "borough": "Manhattan", "raw": "181st Street & St. Nicholas Avenue", "notes": ""}
     ]}),
     ("Which location has fewer open permits: Jamaica Avenue in Queens or Broadway in Upper Manhattan?",
      {"addresses": [
         {"house_number": "", "street_name": "Jamaica Avenue & Parsons Boulevard", "borough": "Queens", "raw": "Jamaica Avenue", "notes": "Jamaica Avenue"},
         {"house_number": "", "street_name": "Broadway & West 125th Street", "borough": "Manhattan", "raw": "Broadway in Upper Manhattan", "notes": "Upper Manhattan"},
     ]}),
    ("Traffic hotspots near Borough Hall and 123 Main St",
     {"addresses": [
         {"house_number": "", "street_name": "Borough Hall", "borough": "Brooklyn", "raw": "Borough Hall", "notes": ""},
         {"house_number": "123", "street_name": "Main St", "borough": "", "raw": "123 Main St", "notes": ""},
     ]}),
    ("Inspections along Canal Street and Bowery",
     {"addresses": [
         {"house_number": "", "street_name": "Canal Street & Bowery", "borough": "Manhattan", "raw": "Canal Street and Bowery", "notes": "Canal Street & Bowery"}
     ]}),
    ("What types of NYPD complaints are most common near Times Square?",
     {"addresses": [
         {"house_number": "", "street_name": "Broadway & 7th Avenue", "borough": "Manhattan", "raw": "Times Square", "notes": "Times Square"}
     ]}),
    ("Collisions around Union Square Park and Penn Station",
     {"addresses": [
         {"house_number": "", "street_name": "14th Street & Broadway", "borough": "Manhattan", "raw": "Union Square Park", "notes": "Union Square Park"},
         {"house_number": "", "street_name": "34th Street & 7th Avenue", "borough": "Manhattan", "raw": "Penn Station", "notes": "Penn Station"},
     ]}),
    ("Incidents by Columbia University and Central Park West",
     {"addresses": [
         {"house_number": "", "street_name": "116th Street & Broadway", "borough": "Manhattan", "raw": "Columbia University", "notes": "Columbia University"},
         {"house_number": "", "street_name": "Central Park West", "borough": "Manhattan", "raw": "Central Park West", "notes": ""},
         {"house_number": "", "street_name": "East 125th Street & Lexington Avenue", "borough": "Manhattan", "raw": "East Harlem", "notes": "East Harlem"},
     ]}),
    ("List recent asbestos control program filings near Court Street in Downtown Brooklyn.",
     {"addresses": [
         {"house_number": "", "street_name": "Court Street & Montague Street", "borough": "Brooklyn", "raw": "Court Street in Downtown Brooklyn", "notes": "Downtown Brooklyn"}
     ]}),
     ("List motor vehicle accidents near The American Museum of Natural History.",
     {"addresses": [
         {"house_number": "200", "street_name": "Central Park West", "borough": "Manhattan", "raw": "The American Museum of Natural History", "notes": "The American Museum of Natural History"}
     ]})
]

# Canonical mapping from risk categories to the datasets that provide answers.
cat_to_ds = {
    "Environmental & Health Risks": [
        "Asbestos Control Program",
        "Population by Neighborhood Tabulation Area",
        "Clean Air Tracking System (CATS)",
        "Citywide Catch Basins",
        "Sewer System Data",
    ],
    "Zoning & Land Use": [
        "City Owned and Leased Property",
        "NYC OpenData Zoning and Tax Lot Database",
        "Historic Districts map",
        "Zoning GIS data",
        "Digital City map shapefile",
        "Parks Monuments",
        "City Owned and Leased Property",
    ],
    "Construction & Permitting": [
        "Street Construction Permits",
        "DOB permits",
        "City Owned and Leased Property",
        "Water and Sewer Permits",
        "DOB NOW: Build - Job Application Findings",
    ],
    "Transportation & Traffic": [
        "NYC OpenData Automated Traffic Volume Counts",
        "NYC OpenData Motor Vehicle Collisions",
        "Crime",
        "Street Construction Permits",
        "MTA subway and other underground train lines",
    ],
    "Public Safety & Social Context": [
        "NYC OpenData Motor Vehicle Collisions",
        "NYC OpenData Automated Traffic Volume Counts",
        "Population by Community Districts",
        "Population by Neighborhood Tabulation Area",
        "Crime",
        "Citywide Hydrants",
    ],
    "Comparative Site Queries": [
        "MTA subway and other underground train lines",
        "Sewer System Data",
        "Water and Sewer Permits",
        "DOB NOW: Build - Job Application Findings",
        "Citywide Hydrants",
    ],
}

# Boroughs and aliases
BOROUGHS = ("Queens", "Manhattan", "Bronx", "Staten Island", "Brooklyn")
_BOROUGH_ALIASES = {
    "queens": "Queens",
    "manhattan": "Manhattan",
    "the bronx": "Bronx",
    "bronx": "Bronx",
    "brooklyn": "Brooklyn",
    "bk": "Brooklyn",
    "staten island": "Staten Island",
    "staten-island": "Staten Island",
    "si": "Staten Island",
}

# Main categories and all datasets
MAIN_CATS = list(cat_to_ds.keys())
ALL_DATASETS = sorted({name for names in cat_to_ds.values() for name in names})

#Dataset Geo Config
DATASET_CONFIG = {
    "Asbestos Control Program": {"geo_unit": "BBL", "mode": "street", "surrounding":True},
    "Crime": {"geo_unit": "PRECINCT", "mode": "street","surrounding":False},
    "Sewer System Data": {"geo_unit": "", "mode": "radius","surrounding":False},
    "Clean Air Tracking System (CATS)": {"geo_unit": "BBL_SPLIT", "mode": "radius","surrounding":True},
    "Population by Community Districts": {"geo_unit": None, "mode": "street","surrounding":False},
    "Population by Neighborhood Tabulation Area": {"geo_unit": "NTA Code", "mode": "street","surrounding":False},
    # Add more dataset configurations as needed
}

# LLM Prompting for returning datasets
SYS_MULTI = f"""Classify the user's question into one or more of these categories:
- Environmental & Health Risks
- Zoning & Land Use
- Construction & Permitting
- Transportation & Traffic
- Public Safety & Social Context
- Comparative Site Queries
Return STRICT JSON only:
{{"categories": ["<labels>"], "datasets": ["<dataset names>"], "confidence": <0..1>}}
Rules:
- Traffic, collisions, congestion, road closures, counts, speeds, hotspots -> Transportation & Traffic
- DOB permits, job filings, street construction, water/sewer permits -> Construction & Permitting
- Zoning, land use, city-owned property, districts -> Zoning & Land Use
- Flood/air/health exposure, sewer systems, population health -> Environmental & Health Risks
- Crime, safety, hydrants, social context -> Public Safety & Social Context
- Comparing between two sites (more/less, better/worse, higher/lower) -> include Comparative Site Queries plus other relevant labels
- "datasets" should list the most relevant NYC datasets (typically 2-5) chosen from: {", ".join(ALL_DATASETS)}.
- Prefer datasets whose categories overlap the predicted categories; only add others when clearly justified by the question.
""".strip()

# LLM Prompt for it to extract locations
SYS_ADDR = """Extract all location mentions from the user's query.
Return STRICT JSON only: {"addresses":[{"house_number":"","street_name":"","borough":"","raw":"","notes":""}, ...]}
Rules:
- Include numbered street addresses (e.g., "10 Jay St", "123 Main Street")
- For intersections output a single object with "street_name" formatted "Street A & Street B"
- Include named places/POIs/landmarks/neighborhoods when used as locators, even without qualifiers
  (e.g., "Times Square", "Union Square Park", "Columbia University", "Penn Station")
- When a broad landmark is mentioned, output the best-known street address or intersection for that landmark.
  Preserve the spoken landmark name in the "notes" field.
- When a neighborhood or area (e.g., "East Harlem") is mentioned, output a representative street address or main intersection within that neighborhood.
- When the query compares two locations ("X versus Y"), output a specific street address or cross street for each location mentioned so downstream systems can geocode them.
  If historic data suggests a canonical address for the location, return that; otherwise pick a central block or intersection.
- If the query says "near X", include X
- Do NOT include cities/states/countries unless explicitly mentioned
- Preserve original wording/casing in the "raw" field; trim whitespace elsewhere
- Deduplicate while preserving order
Field guidance:
- house_number: leading digits for numbered addresses; leave empty string if none
- street_name: primary street/highway name or POI/intersection label (use intersections for landmarks when appropriate)
- borough: Choose from Queens, Manhattan, Bronx, Staten Island, Brooklyn when determinable; otherwise empty
- notes: optional clarifications (e.g., original landmark wording). Use empty string when not needed
""".strip()

def check_env():
    missing = [k for k, v in globals().items() if k.isupper() and v is None]
    if missing:
        print("Missing environment variables:", missing)
    else:
        print("All environment variables loaded properly")

if __name__ == "__main__":
    check_env()
