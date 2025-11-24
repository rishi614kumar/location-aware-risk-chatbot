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
   # "Population by Community Districts": (
   #     "Aggregated population counts by community district for decennial census years "
    #    "1970 through 2010, used for demographic and spatial trend analysis."
   # ),
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
        "The Agency performs ongoing assessment of New York City streets. Ratings are based on a scale from 1 to 10."
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

# Default domain to call SOCRATA
DEFAULT_SOCRATA_DOMAIN = "data.cityofnewyork.us"

# Alternative domain
SOCRATA_DOMAIN_OVERRIDES = {
    "MTA subway and other underground train lines": "data.ny.gov",
}

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
    # "Population by Community Districts" : "xi7c-iiu2",
    "Population by Neighborhood Tabulation Area" : "swpk-hqdp",
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
    # --- Transportation & Traffic ---
    (
        "Where are the top traffic accident hotspots within 500 feet of 163rd Street?",
        {
            "categories": ["Transportation & Traffic"],
            "datasets": [
                "NYC OpenData Motor Vehicle Collisions",
                "NYC OpenData Automated Traffic Volume Counts"
            ],
            "confidence": 0.90,
            "borough": "Bronx",
        }
    ),

    # --- Construction & Permitting ---
    (
        "Any active DOB permits near 10 Jay St?",
        {
            "categories": ["Construction & Permitting"],
            "datasets": [
                "DOB permits"
            ],
            "confidence": 0.90,
            "borough": "Brooklyn",
        }
    ),

    # --- Zoning & Land Use ---
    (
        "Is West 4th St & Washington Square East in a historic district and any zoning information?",
        {
            "categories": ["Zoning & Land Use"],
            "datasets": [
                "Historic Districts map",
                "NYC OpenData Zoning and Tax Lot Database"
            ],
            "confidence": 0.90,
            "borough": "Manhattan",
        }
    ),

    # --- Environmental & Health Risks ---
    (
        "Any flood or sewer risk around 123 Main St?",
        {
            "categories": ["Environmental & Health Risks"],
            "datasets": [
                "Sewer System Data",
                "Citywide Catch Basins"
            ],
            "confidence": 0.82,
            "borough": "Queens",
        }
    ),

    # --- Public Safety & Social Context ---
    (
        "Where are the nearest fire hydrants near Borough Hall?",
        {
            "categories": ["Public Safety & Social Context"],
            "datasets": [
                "Citywide Hydrants"
            ],
            "confidence": 0.78,
            "borough": "Brooklyn",
        }
    ),

    # --- Comparative: Zoning + Environmental ---
    (
        "Compare zoning and environmental risks for 149th Street & Grand Concourse versus 181st Street & St. Nicholas Avenue.",
        {
            "categories": ["Comparative Site Queries", "Zoning & Land Use", "Environmental & Health Risks"],
            "datasets": [
                "NYC OpenData Zoning and Tax Lot Database",
                "Sewer System Data"
            ],
            "confidence": 0.90,
            "borough": "Manhattan",
        }
    ),

    # --- Comparative: Population + Crime ---
    (
        "What is the population and crime statistics near Financial District compared to Hell's Kitchen?",
        {
            "categories": ["Comparative Site Queries", "Public Safety & Social Context"],
            "datasets": [
                "Population by Neighborhood Tabulation Area",
                "Crime"
            ],
            "confidence": 0.86,
            "borough": "Manhattan",
        }
    )
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
    ("Show the latest traffic counts for East 34th Street between 5th Avenue and Madison Avenue.",
     {"addresses": [
         {"house_number": "", "street_name": "East 34th Street & 5th Avenue", "borough": "Manhattan", "raw": "East 34th Street between 5th Avenue and Madison Avenue", "notes": "Segment: East 34th Street between 5th Ave and Madison Ave"},
         {"house_number": "", "street_name": "East 34th Street & Madison Avenue", "borough": "Manhattan", "raw": "East 34th Street between 5th Avenue and Madison Avenue", "notes": "Segment: East 34th Street between 5th Ave and Madison Ave"}
     ]}),
    ("Compare traffic volumes between 240 E 38th St and 237 Park Ave.",
     {"addresses": [
         {"house_number": "240", "street_name": "East 38th St", "borough": "", "raw": "240 E 38th St", "notes": ""},
         {"house_number": "237", "street_name": "Park Ave", "borough": "Manhattan", "raw": "237 Park Ave", "notes": ""}
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
     ]}),
    ("Is 120th Street and 5th Avenue in Astoria, an MS4 zone, or location of nearby catch basins?",
     {"addresses": [
         {"house_number": "", "street_name": "120th Street & 5th Avenue", "borough": "Manhattan", "raw": "120th Street and 5th Avenue in Astoria", "notes": "User specified 'in Astoria' for this intersection which is factually i...e intersection of 120th Street and 5th Avenue is in Manhattan."}
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
        "MTA subway and other underground train lines"
        "Street Pavement Rating",
    ],
    "Public Safety & Social Context": [
        "NYC OpenData Motor Vehicle Collisions",
        "NYC OpenData Automated Traffic Volume Counts",
        # "Population by Community Districts",
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
    #"Population by Community Districts": {"geo_unit": None, "mode": "street","surrounding":False},
    "Population by Neighborhood Tabulation Area": {"geo_unit": "NTA Code", "mode": "street","surrounding":False},
    # Add more dataset configurations as needed
}

# LLM Prompting for returning datasets
SYS_MULTI = f"""
Classify the user's question into one or more of the following categories:
- Environmental & Health Risks
- Zoning & Land Use
- Construction & Permitting
- Transportation & Traffic
- Public Safety & Social Context
- Comparative Site Queries

Return STRICT JSON only in this format:
{{
  "categories": ["<labels>"],
  "datasets": ["<dataset names>"],
  "confidence": <0..1>
}}

Core rules:
1. Select the **minimal number of datasets** required to directly answer the question.
2. Only choose datasets whose categories clearly match the user's intent.
3. Do NOT include datasets from other categories unless explicitly justified by the wording.
4. Use only canonical dataset names from this list: {", ".join(ALL_DATASETS)}.
5. Category routing:
   - Traffic, crashes, congestion, counts, speeds, hotspots → Transportation & Traffic
   - DOB permits, job filings, street construction, water/sewer permits → Construction & Permitting
   - Zoning, land use, districts, city-owned property → Zoning & Land Use
   - Flooding, sewer backups, environmental exposure, air/health risks → Environmental & Health Risks
   - Population, crime, hydrants, social context → Public Safety & Social Context
   - Comparisons between two locations (better/worse, higher/lower) → include Comparative Site Queries + relevant categories
6. Population-only questions should use **population datasets**, not environmental or crime datasets.
7. When multiple categories apply, return only the datasets from the selected categories that directly answer the question.
"""


# LLM Prompt for it to extract locations
SYS_ADDR = """Extract all NYC location mentions from the user's query.

Return STRICT JSON only:
{"addresses":[{"house_number":"","street_name":"","borough":"","raw":"","notes":""}, ...]}

Rules:
- Include numbered street addresses (e.g., "10 Jay St", "123 Main Street").
- For intersections output a single object with "street_name" formatted "Street A & Street B"
  and house_number="".
- Include named places/POIs/landmarks/neighborhoods when used as locators, even without qualifiers
  (e.g., "Times Square", "Union Square Park", "Columbia University", "Penn Station").
- If a broad landmark is mentioned, output its best-known NYC address or intersection.
  Preserve the spoken landmark name in "notes".
- If a neighborhood/area (e.g., "East Harlem") is mentioned, output a representative central
  intersection or address only if needed for geocoding. Mark this as an approximation in "notes".
- If the query compares two locations ("X versus Y"), output one specific address or intersection
  for each side. If you must approximate, say so in "notes".
- If the query describes a stretch like "Street A between Street B and Street C", emit two entries:
  Street A & Street B and Street A & Street C. Keep the original wording in "notes"
  (e.g., "Street A between Street B and Street C").
- If the query says "near X", include X as a location.
- Do NOT include cities/states/countries unless explicitly mentioned.
- Preserve original wording/casing in "raw"; trim whitespace elsewhere.
- Deduplicate while preserving order.

Borough guidance:
- borough must be one of: Queens, Manhattan, Bronx, Staten Island, Brooklyn.
- Fill borough if explicitly stated OR unambiguously inferable from the location.
- If the user provides a neighborhood/borough that conflicts with a well-known NYC street/intersection,
  **correct the borough to the true one** and explain the correction briefly in notes.
- If still ambiguous (street exists in multiple boroughs and no strong clue), leave borough=""
  and note ambiguity in "notes". Do NOT guess in that case.

Field guidance:
- house_number: leading digits for numbered addresses; "" if none.
- street_name: primary street/highway name or POI/intersection label.
- raw: exact substring from user query.
- notes: clarifications like original landmark name or "approximate"/"ambiguous"/"conflict". Use "" if not needed.
""".strip()



def check_env():
    missing = [k for k, v in globals().items() if k.isupper() and v is None]
    if missing:
        print("Missing environment variables:", missing)
    else:
        print("All environment variables loaded properly")

if __name__ == "__main__":
    check_env()
