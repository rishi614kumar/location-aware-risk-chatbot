import pytest
from adapters import coords
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

class DummyPluto:
    def __init__(self):
        self.crs = {'init': 'epsg:2263'}
        self.data = pd.DataFrame({
            'BBL': ['1', '2'],
            'geometry': [Point(1, 2), Point(3, 4)]
        })
        self.sindex = self
    def __getitem__(self, key):
        return self.data[key]
    def iloc(self, idxs):
        return self.data.iloc[idxs]
    def intersects(self, pt):
        return pd.Series([True, False])
    def query(self, pt, predicate=None):
        return [0]
    def empty(self):
        return False

def dummy_load_pluto_geom():
    df = gpd.GeoDataFrame({
        'BBL': ['1'],
        'geometry': [Point(987654, 123456)]
    }, crs='epsg:2263')
    return df

def test_get_bbl_from_lonlat(monkeypatch):
    monkeypatch.setattr(coords, 'load_pluto_geom', dummy_load_pluto_geom)
    monkeypatch.setattr(coords, 'get_lonlat_to_stateplane', lambda lon, lat: (987654, 123456))
    result = coords.get_bbl_from_lonlat(-73.9, 40.7)
    assert result is not None
    assert isinstance(result[0], str)
    assert isinstance(result[1], float)

def test_get_bbls_near_lonlat(monkeypatch):
    monkeypatch.setattr(coords, 'load_pluto_geom', dummy_load_pluto_geom)
    monkeypatch.setattr(coords, 'get_lonlat_to_stateplane', lambda lon, lat: (987654, 123456))
    result = coords.get_bbls_near_lonlat(-73.9, 40.7)
    assert isinstance(result, list)

def test_get_lonlat_from_bbl(monkeypatch):
    monkeypatch.setattr(coords, 'load_pluto_geom', dummy_load_pluto_geom)
    monkeypatch.setattr(coords, 'get_stateplane_to_lonlat', lambda x, y: (-73.9, 40.7))
    result = coords.get_lonlat_from_bbl('1')
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert all(isinstance(x, float) for x in result)
