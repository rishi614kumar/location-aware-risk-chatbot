import pytest
from adapters import nta
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

def dummy_load_pluto_geom():
    df = gpd.GeoDataFrame({
        'BBL': ['1'],
        'geometry': [Point(1, 2)]
    }, crs='epsg:2263')
    return df

def dummy_load_nta_2020():
    df = gpd.GeoDataFrame({
        'NTA_CODE': ['MN01'],
        'geometry': [Point(1, 2).buffer(1)]
    }, crs='epsg:2263')
    return df

def test_get_nta_from_bbl(monkeypatch):
    monkeypatch.setattr(nta, 'load_pluto_geom', dummy_load_pluto_geom)
    monkeypatch.setattr(nta, 'load_nta_2020', dummy_load_nta_2020)
    result = nta.get_nta_from_bbl('1')
    assert result == 'MN01'

def test_get_bbls_from_nta(monkeypatch):
    monkeypatch.setattr(nta, 'load_pluto_geom', dummy_load_pluto_geom)
    monkeypatch.setattr(nta, 'load_nta_2020', dummy_load_nta_2020)
    result = nta.get_bbls_from_nta('MN01')
    assert isinstance(result, list)
    assert '1' in result
