from __future__ import annotations
from typing import Optional, List
from data.pluto import load_pluto_lookup


# ---- public API ----
def get_precinct_from_bbl(bbl: str) -> Optional[int]:
    """
    Returns the precinct number for a BBL (int) or None if missing.
    """
    pluto = load_pluto_lookup()
    row = pluto.loc[pluto["BBL"] == bbl]
    if row.empty:
        return None
    val = row.iloc[0]["PolicePrct"]
    return int(val) if val is not None else None

def get_bbls_from_precinct(precinct_number: int) -> List[str]:
    """
    Returns all BBLs within a given precinct as a list[str].
    """
    pluto = load_pluto_lookup()
    sub = pluto.loc[pluto["PolicePrct"] == precinct_number, "BBL"]
    return [str(b) for b in sub.unique()]