from typing import List
import pandas as pd
import geopandas as gpd
from data.pluto import load_pluto_lookup, load_pluto_geom


def get_nta_from_bbl(bbl: str) -> List[str]:
    """
    BBL -> NTA: Returns the NTA code(s) containing this BBL.
    
    This is achieved using a fast attribute lookup on the cached PLUTO data,
    which includes the NTA code as a property of the tax lot.
    """
    # PLUTO data loaded for fast attribute lookup (does not need geometry)
    pluto_lookup_df = load_pluto_lookup()
    
    # Ensure BBL is a string for lookup
    bbl_str = str(bbl)
    
    # PLUTO's primary key column is 'BBL'. The NTA column is named 'NTA2020'.
    # We filter the DataFrame where the BBL matches the input.
    result = pluto_lookup_df.loc[pluto_lookup_df['BBL'] == bbl_str, 'NTA2020']
    
    # If the result is empty, return an empty list. Otherwise, return unique values.
    if result.empty:
        return []
    
    # Return the list of NTA codes (usually one, but a complex BBL might yield multiple)
    return result.astype(str).unique().tolist()


def get_bbls_from_nta(nta_code: str) -> List[str]:
    """
    NTA -> BBLs: Returns all BBLs (tax lots) whose geometry falls within the 
    specified NTA region, using a spatial join.
    """
    # 1. Load the PLUTO data with geometry for spatial operations
    # This dataset contains BBL polygons (the "s.join" target)
    pluto_geom_gdf = load_pluto_geom() 

    # 2. Load the NTA boundary data (the "s.join" source/filter)
    # For robust production code, we'll download the official NTA boundary GeoJSON.
    NTA_BOUNDARY_URL = "https://data.cityofnewyork.us/resource/9nt8-h7nd.geojson"
    
    try:
        # Load the NTA boundaries as a GeoDataFrame
        nta_boundaries_gdf = gpd.read_file(NTA_BOUNDARY_URL)
        
    except Exception as e:
        # Handle cases where the URL or GeoJSON loading fails
        print(f"Error loading NTA boundaries for spatial join: {e}")
        return []

    # 3. Filter the NTA boundaries to isolate the target NTA region
    # The column for the NTA code in this file is 'NTA2020'
    target_nta_gdf = nta_boundaries_gdf[nta_boundaries_gdf['NTA2020'] == nta_code]
    
    if target_nta_gdf.empty:
        print(f"NTA code '{nta_code}' not found in boundary data.")
        return []

    # 4. Perform the Spatial Join (sjoin)
    # Join BBL polygons (pluto_geom_gdf) to the NTA polygon (target_nta_gdf)
    # op='within' means we only keep BBLs that are ENTIRELY within the NTA geometry. 
    # op='intersects' might be safer for BBLs on the boundary, but 'within' is often cleaner.
    
    # Ensure both GeoDataFrames use the same CRS (Coordinate Reference System)
    target_nta_gdf = target_nta_gdf.to_crs(pluto_geom_gdf.crs)

    joined_result = gpd.sjoin(
        pluto_geom_gdf,
        target_nta_gdf,
        how="inner",
        op="within" 
    )

    # 5. Extract the BBLs
    # The primary key column in PLUTO is 'BBL'
    bbl_list = joined_result['BBL'].astype(str).unique().tolist()
    
    return bbl_list
