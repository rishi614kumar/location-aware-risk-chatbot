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


ALL_CASES = TEST_ADDR_PARSING


# ------------------ Tests ------------------

@pytest.fixture(scope="module")
def parser():
    return get_default_parser(provider="gemini")


@pytest.mark.parametrize("_label, query, expected", ALL_CASES)
def test_extract_addresses(parser, _label, query, expected):
    result = parser.extract_addresses(query)
    assert addresses_equal_ignore_notes(result, expected)
