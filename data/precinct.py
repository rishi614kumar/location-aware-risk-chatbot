import geopandas as gpd
import fiona
from pathlib import Path

GDB_PATH = Path(r"C:\Users\Rishik\Documents\CAPSTONE\nyc_mappluto_25v2_1_fgdb\MapPLUTO25v2_1.gdb")  

def load_pluto_from_gdb(gdb_path=GDB_PATH) -> gpd.GeoDataFrame:
    layers = fiona.listlayers(gdb_path)
    print("layers:", layers)

    layer_name = [lyr for lyr in layers if "pluto" in lyr.lower()][0]

    gdf = gpd.read_file(gdb_path, layer=layer_name)

    cols_lower = {c.lower(): c for c in gdf.columns}
    bbl_col = cols_lower.get("bbl")
    prct_col = cols_lower.get("policeprct") or cols_lower.get("police_prct") or cols_lower.get("policepct")

    if bbl_col is None:
        raise ValueError("no bbl")
    if prct_col is None:
        raise ValueError("no precinct")

    gdf = gdf.rename(columns={bbl_col: "BBL", prct_col: "PolicePrct"})
    return gdf[["BBL", "PolicePrct", "geometry"]]

# BBL -> Precinct 
def bbl_to_precinct(pluto_gdf: gpd.GeoDataFrame, bbls):
    if isinstance(bbls, str):
        bbls = [bbls]
    sub = pluto_gdf.loc[pluto_gdf["BBL"].isin(bbls), ["BBL", "PolicePrct"]].copy()
    return {str(r.BBL): (int(r.PolicePrct) if r.PolicePrct is not None else None)
            for _, r in sub.iterrows()}

# Precinct -> BBLs 
def precinct_to_bbls(pluto_gdf: gpd.GeoDataFrame, precinct_number: int):
    sub = pluto_gdf.loc[pluto_gdf["PolicePrct"] == precinct_number, ["BBL"]]
    return [str(b) for b in sub["BBL"].unique()]

if __name__ == "__main__":
    pluto = load_pluto_from_gdb(GDB_PATH)

    print(bbl_to_precinct(pluto, "1001720032"))

    bbls = precinct_to_bbls(pluto, 25)
    print("count:", len(bbls))
    print("bbls:", bbls)
