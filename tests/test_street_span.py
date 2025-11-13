import pytest
from adapters import street_span
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Polygon

def dummy_load_pluto_geom():
    df = gpd.GeoDataFrame({
        'BBL': ['1'],
        'geometry': [Polygon([(0,0),(1,0),(1,1),(0,1)])]
    }, crs='epsg:2263')
    return df

def dummy_load_lion_geom():
    df = gpd.GeoDataFrame({
        '_street_name': ['BROADWAY'],
        '_width_ft': [30],
        'geometry': [LineString([(0,0),(1,1)])]
    }, crs='epsg:2263')
    return df

def test_get_bbls_from_lion_span(monkeypatch):
    monkeypatch.setattr(street_span, 'load_pluto_geom', dummy_load_pluto_geom)
    monkeypatch.setattr(street_span, 'load_lion_geom', dummy_load_lion_geom)
    result = street_span.get_bbls_from_lion_span('BROADWAY')
    assert isinstance(result, list)
    assert '1' in result

def test_get_lion_span_from_bbl(monkeypatch):
    monkeypatch.setattr(street_span, 'load_pluto_geom', dummy_load_pluto_geom)
    monkeypatch.setattr(street_span, 'load_lion_geom', dummy_load_lion_geom)
    result = street_span.get_lion_span_from_bbl('1')
    assert isinstance(result, str) or result is None
