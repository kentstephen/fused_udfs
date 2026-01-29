# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, buffer_multiple: float = 1):
    import shapely
    import geopandas as gpd
    import numpy as np
    import pandas as pd
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/0b1bd10/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tiles = common.get_tiles(bounds, target_num_tiles=20)
    # Buffering tiles internally
    tiles.geometry = tiles.buffer(buffer_multiple / (tiles.z.iloc[0]) ** 2)
    # Use print statements to debug
    return tiles
    # print(f"{tiles.geometry.area.sample(3)=}")
    # gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=tiles, overture_type='segment', min_zoom=0)
    # if gdf is None or gdf.empty:
    #     return pd.DataFrame({})
    
    # # Define walking modes/types
    # walking_types = ['walking', 'pedestrian', 'footway', 'path', 'cycleway', 'sidewalk']
    
    # # Filter the GeoDataFrame to include only walking segments
    # gdf = gdf[gdf['class'].isin(walking_types)]
    
    # # Calculate the length of EACH geometry in meters
    # gdf['metric'] = gdf.to_crs('EPSG:3857').geometry.length
    
    # # Apply log normalization to each individual segment length
    # # Only do this if the values are very large and need normalization
    # gdf['metric'] = np.log10(gdf['metric'])
    
    # # Select only the geometry and metric columns
    # gdf = gdf[['geometry', 'metric']]
    # return gdf