@fused.cache
def get_mask():
    import shapely
    import geopandas as gpd
    import pandas as pd
    
    nh_mask = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/33_NEW_HAMPSHIRE/33/tl_2020_33_tract20.zip')
    me_mask = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/23_MAINE/23/tl_2020_23_tract20.zip')
    vt_mask = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/50_VERMONT/50/tl_2020_50_tract20.zip')
    
    # Combine all and dissolve into a single geometry
    combined = gpd.GeoDataFrame(pd.concat([vt_mask])).dissolve()
    
    # Create a new GeoDataFrame with just the dissolved geometry
    return gpd.GeoDataFrame(geometry=[combined.geometry.iloc[0]], crs=combined.crs)