import pytest
from llm.LLMParser import get_default_parser


# ------------------ Helper: compare while ignoring notes ------------------

def addresses_equal_ignore_notes(a, b):
    """Compare two lists of address dicts ignoring differences in the `notes` field."""
    if len(a) != len(b):
        return False

    for addr_a, addr_b in zip(a, b):
        for key in ("house_number", "street_name", "borough", "raw"):
            if addr_a.get(key) != addr_b.get(key):
                return False
        # notes is ignored entirely

    return True


# ------------------ Consolidated Address Parsing Tests ------------------

TEST_ADDR_PARSING = [
    (
        "Specific address",
        "Any active DOB permits near 10 Jay St?",
        [
            {
                "house_number": "10",
                "street_name": "Jay St",
                "borough": "Brooklyn",
                "raw": "10 Jay St",
                "notes": "",  # LLM may return "" or "Jay Street"
            }
        ],
    ),

    (
        "Intersection",
        "Show recent crash reports around 14th Street and 7th Avenue",
        [
            {
                "house_number": "",
                "street_name": "14th Street & 7th Avenue",
                "borough": "Manhattan",
                "raw": "14th Street and 7th Avenue",
                "notes": "",
            }
        ],
    ),

    (
        "Landmark canonical",
        "What types of NYPD complaints are most common near Times Square?",
        [
            {
                "house_number": "",
                "street_name": "Broadway & 7th Avenue",
                "borough": "Manhattan",
                "raw": "Times Square",
                "notes": "Times Square",  # but test WON'T enforce this
            }
        ],
    ),

    (
        "Neighborhood representative",
        "Assess flood risk in East Harlem",
        [
            {
                "house_number": "",
                "street_name": "East 125th Street & Lexington Avenue",
                "borough": "Manhattan",
                "raw": "East Harlem",
                "notes": "East Harlem",  # but test ignores notes
            }
        ],
    ),

    (
        "Queens hyphenated address",
        "Are there asbestos filings near 45-10 21st Street in Queens?",
        [
            {
                "house_number": "45-10",
                "street_name": "21st Street",
                "borough": "Queens",
                "raw": "45-10 21st Street in Queens",
                "notes": "",
            }
        ],
    ),

    # (
    #     "Street corridor",
    #     "Where are the traffic accident hotspots along 163rd Street?",
    #     [
    #         {
    #             "house_number": "",
    #             "street_name": "163rd Street",
    #             "borough": "Bronx",
    #             "raw": "163rd Street",
    #             "notes": "",
    #         }
    #     ],
    # ),

    (
        "Compare intersections",
        "Compare zoning at 149th Street & Grand Concourse versus 181st Street & St. Nicholas Avenue.",
        [
            {
                "house_number": "",
                "street_name": "149th Street & Grand Concourse",
                "borough": "Bronx",
                "raw": "149th Street & Grand Concourse",
                "notes": "",
            },
            {
                "house_number": "",
                "street_name": "181st Street & St. Nicholas Avenue",
                "borough": "Manhattan",
                "raw": "181st Street & St. Nicholas Avenue",
                "notes": "",
            },
        ],
    ),

    (
        "Compare neighborhoods",
        "How does population density compare between Jackson Heights and Downtown Brooklyn?",
        [
            {
                "house_number": "",
                "street_name": "37th Avenue & 74th Street",
                "borough": "Queens",
                "raw": "Jackson Heights",
                "notes": "Jackson Heights",
            },
            {
                "house_number": "",
                "street_name": "Fulton Street & Jay Street",
                "borough": "Brooklyn",
                "raw": "Downtown Brooklyn",
                "notes": "Downtown Brooklyn",
            },
        ],
    ),

    (   
        "MS4 zone example but borough specified wrong -> correct it",
        "Is 120th Street and 5th Avenue in Astoria, an MS4 zone, or location of nearby catch basins?",
        [
            {
                "house_number": "",
                "street_name": "120th Street & 5th Avenue",
                "borough": "Manhattan",
                "raw": "120th Street and 5th Avenue in Astoria",
                "notes": "User specified 'in Astoria' for this intersection, but is actually in Manhattan.",
            }
        ],
    ),

    (
        "Env brownfield intersection",
        "Are there brownfield cleanup sites near 14th Street and Avenue C?",
        [
            {
                "house_number": "",
                "street_name": "14th Street & Avenue C",
                "borough": "Manhattan",
                "raw": "14th Street and Avenue C",
                "notes": "",
            }
        ],
    ),

    (
        "Noise complaints intersection",
        "Show recent noise complaints within 0.25 miles of 125th Street and 7th Avenue.",
        [
            {
                "house_number": "",
                "street_name": "125th Street & 7th Avenue",
                "borough": "Manhattan",
                "raw": "125th Street and 7th Avenue",
                "notes": "",
            }
        ],
    ),

    (
        "Blank",
        "Give me a citywide summary",
        [],
    ),
]


TEST_SEGMENT_STREET_CASES = [

    # --- "between" phrasing ---
    (
        "Midtown corridor segment",
        "Show me the traffic counts for East 34th Street between 5th Avenue and Madison Avenue for the latest year in the dataset, and summarise what that indicates about congestion in Midtown",
        [
            {
                "house_number": "",
                "street_name": "East 34th Street & 5th Avenue",
                "borough": "Manhattan",
                "raw": "East 34th Street between 5th Avenue and Madison Avenue",
                "notes": "Segment: East 34th Street between 5th Avenue and Madison Avenue",
            },
            {
                "house_number": "",
                "street_name": "East 34th Street & Madison Avenue",
                "borough": "Manhattan",
                "raw": "East 34th Street between 5th Avenue and Madison Avenue",
                "notes": "Segment: East 34th Street between 5th Avenue and Madison Avenue",
            },
        ],
    ),

    # --- "from X to Y" phrasing ---
    (
        "Flatbush corridor segment (from-to)",
        "Analyze traffic volumes for Flatbush Avenue from Atlantic Avenue to Tillary Street for the most recent year available.",
        [
            {
                "house_number": "",
                "street_name": "Flatbush Avenue & Atlantic Avenue",
                "borough": "Brooklyn",
                "raw": "Flatbush Avenue from Atlantic Avenue to Tillary Street",
                "notes": "Segment: Flatbush Avenue from Atlantic Avenue to Tillary Street",
            },
            {
                "house_number": "",
                "street_name": "Flatbush Avenue & Tillary Street",
                "borough": "Brooklyn",
                "raw": "Flatbush Avenue from Atlantic Avenue to Tillary Street",
                "notes": "Segment: Flatbush Avenue from Atlantic Avenue to Tillary Street",
            },
        ],
    ),

    # --- "spanning" phrasing ---
    (
        "Queens Blvd spanning segment",
        "Provide the latest traffic counts for the stretch of Queens Boulevard spanning 50th Street to 55th Street.",
        [
            {
                "house_number": "",
                "street_name": "Queens Boulevard & 50th Street",
                "borough": "Queens",
                "raw": "Queens Boulevard spanning 50th Street to 55th Street",
                "notes": "Segment: Queens Boulevard spanning 50th Street to 55th Street",
            },
            {
                "house_number": "",
                "street_name": "Queens Boulevard & 55th Street",
                "borough": "Queens",
                "raw": "Queens Boulevard spanning 50th Street to 55th Street",
                "notes": "Segment: Queens Boulevard spanning 50th Street to 55th Street",
            },
        ],
    ),

    # --- "bounded by" phrasing ---
    (
        "Broadway bounded segment",
        "Get traffic volume statistics for Broadway bounded by West 72nd Street and West 79th Street.",
        [
            {
                "house_number": "",
                "street_name": "Broadway & West 72nd Street",
                "borough": "Manhattan",
                "raw": "Broadway bounded by West 72nd Street and West 79th Street",
                "notes": "Segment: Broadway bounded by West 72nd Street and West 79th Street",
            },
            {
                "house_number": "",
                "street_name": "Broadway & West 79th Street",
                "borough": "Manhattan",
                "raw": "Broadway bounded by West 72nd Street and West 79th Street",
                "notes": "Segment: Broadway bounded by West 72nd Street and West 79th Street",
            },
        ],
    ),


    # --- "corridor along" phrasing ---
    (
        "7th Avenue corridor along",
        "Show traffic counts for the corridor along 7th Avenue from West 23rd Street to West 30th Street.",
        [
            {
                "house_number": "",
                "street_name": "7th Avenue & West 23rd Street",
                "borough": "Manhattan",
                "raw": "7th Avenue from West 23rd Street to West 30th Street",
                "notes": "Segment: 7th Avenue from West 23rd Street to West 30th Street",
            },
            {
                "house_number": "",
                "street_name": "7th Avenue & West 30th Street",
                "borough": "Manhattan",
                "raw": "7th Avenue from West 23rd Street to West 30th Street",
                "notes": "Segment: 7th Avenue from West 23rd Street to West 30th Street",
            },
        ],
    ),

]

ALL_CASES = TEST_ADDR_PARSING + TEST_SEGMENT_STREET_CASES


# ------------------ Tests ------------------

@pytest.fixture(scope="module")
def parser():
    return get_default_parser(provider="gemini")


@pytest.mark.parametrize("_label, query, expected", ALL_CASES)
def test_extract_addresses(parser, _label, query, expected):
    result = parser.extract_addresses(query)
    assert addresses_equal_ignore_notes(result, expected)
