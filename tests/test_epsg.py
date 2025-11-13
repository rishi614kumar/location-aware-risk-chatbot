import pytest
from adapters import epsg

def test_get_lonlat_to_stateplane():
    x, y = epsg.get_lonlat_to_stateplane(-73.9857, 40.7484)
    assert isinstance(x, float) and isinstance(y, float)

def test_get_stateplane_to_lonlat():
    lon, lat = epsg.get_stateplane_to_lonlat(987654, 123456)
    assert isinstance(lon, float) and isinstance(lat, float)
