# tests/test_geobundle.py
import os
import pytest
from scripts.geobundle import geo_from_address, geo_from_bbl
from adapters.schemas import GeoBundle

@pytest.mark.skipif(not os.getenv("GEOCLIENT_API_KEY"), reason="Needs Geoclient key")
def test_geo_from_address_shape():
    b = geo_from_address("237 Park Ave", "Manhattan")
    assert isinstance(b, GeoBundle)
    assert b.bbl is not None
    assert b.sources.geoclient is not None  # precinct should come from geoclient in address flow

def test_geo_from_bbl_shape():
    b = geo_from_bbl("1013007501")  # known good BBL
    assert isinstance(b, GeoBundle)
    # precinct can be None if PLUTO row missing, but bundle still valid
    assert b.bbl == "1013007501"
