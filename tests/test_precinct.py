import pytest
from adapters import precinct
import pandas as pd

def dummy_load_pluto_lookup():
    df = pd.DataFrame({
        'BBL': ['1', '2'],
        'PolicePrct': [10, 20]
    })
    return df

def test_get_precinct_from_bbl(monkeypatch):
    monkeypatch.setattr(precinct, 'load_pluto_lookup', dummy_load_pluto_lookup)
    result = precinct.get_precinct_from_bbl('1')
    assert result == 10
    result_none = precinct.get_precinct_from_bbl('999')
    assert result_none is None

def test_get_bbls_from_precinct(monkeypatch):
    monkeypatch.setattr(precinct, 'load_pluto_lookup', dummy_load_pluto_lookup)
    result = precinct.get_bbls_from_precinct(10)
    assert isinstance(result, list)
    assert '1' in result
