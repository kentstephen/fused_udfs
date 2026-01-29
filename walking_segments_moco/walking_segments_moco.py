@fused.udf
def udf(bounds: fused.types.Tile = None,
        n: int=10):
    import geopandas as gpd
    import shapely
    import pandas as pd

    def get_segments(bounds):

        overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
   
        gdf= overture_utils.get_overture(bounds=bounds, overture_type='segment', min_zoom=0)
        if gdf is None or gdf.empty:
            return pd.DataFrame({})
        # Define walking modes/types (adjust based on your specific data categories)
        walking_types = ['walking', 'pedestrian', 'footway', 'path', 'cycleway','sidewalk']
        # Filter the GeoDataFrame to include only walking segments
        gdf = gdf[gdf['class'].isin(walking_types)]
        # gdf = gpd.overlay(gdf, mask, how='intersection')
        return gdf
    gdf_overture = get_segments(bounds)
    return gdf_overture