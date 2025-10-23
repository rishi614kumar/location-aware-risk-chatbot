from dotenv import load_dotenv
import os

load_dotenv()

GEOCLIENT_API_KEY= os.getenv("GEOCLIENT_API_KEY")
MAPPLUTO_GDB_PATH = os.getenv("MAPPLUTO_GDB_PATH")
LION_GDB_PATH = os.getenv("LION_GDB_PATH")
NTA_PATH = os.getenv("NTA_PATH") # https://data.cityofnewyork.us/resource/9nt8-h7nd.geojson

# STREET_SPAN SETTINGS
MAX_BUFFER_FT = 120  # Maximum buffer distance in feet
MIN_BUFFER_FT = 10   # Minimum buffer distance in feet
DEFAULT_BUFFER_INCREMENT_FT = 10  # Default increment to add to street width
DEFAULT_BUFFER_FT = 30  # Default buffer distance when street width is unknown
# If we don’t know the street width, assume DEFAULT_BUFFER_FT ft. Otherwise, take the width plus DEFAULT_BUFFER_INCREMENT_FT ft of margin, but keep it between MIN_BUFFER_FT and MAX_BUFFER_FT ft total.

# Human-readable context for each dataset; extend as new sources come online.
DATASET_DESCRIPTIONS = {
    "Asbestos Control Program": (
        "ACP7 form is an asbestos project notification. Any time asbestos abatement is perform on "
        "quantities greater than a minor project amount, the applicant has to file this form with "
        "DEP Asbestos Control Program (ACP). All asbestos documents are filed through the Asbestos "
        "Reporting and Tracking System (ARTS) E-file system. This system is web based and entirely "
        "paperless. All information on the ACP7 is essential to meet the requirements setforth in "
        "the asbestos rules and regulations Title15, Chapter 1 (RCNY). ACP enforcement staff utilizes "
        "this form for inspection of asbestos abatement activities."
    ),
    "Citywide Catch Basins": (
        "NYCDEP Citywide Catch Basins. "
        "Catch basins are an important part of New York City’s 7,500-mile sewer network. "
        "They are connected to underground pipes that channel stormwater from the street "
        "to one of DEP’s 14 wastewater resource recovery facilities, or directly into our "
        "surrounding waterbodies. DEP cleans and maintains over 150,000 catch basins citywide."
    )
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
    "Asbestos Control Program": "vq35-j9qm",
    "Clean Air Tracking System (CATS)": "f4rp-2kvy",
}

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
        "LION data",
        "Zoning GIS data",
        "Digital City map shapefile",
        "Parks Monuments",
        "City Owned and Leased Property",
    ],
    "Construction & Permitting": [
        "Street Construction Permits",
        "DOB permits",
        "City Owned and Leased Property",
        "DOB Job filings",
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
        "NYC OpenData PLUTO",
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
    "Asbestos Control Program": {"geo_field": "BBL"},
}

def check_env():
    missing = [k for k, v in globals().items() if k.isupper() and v is None]
    if missing:
        print("Missing environment variables:", missing)
    else:
        print("All environment variables loaded properly")

if __name__ == "__main__":
    check_env()