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
    # print(f"{tiles.geometry.area.sample(3)=}")
    gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=tiles, overture_type='segment',min_zoom=0)
    if gdf is None or gdf.empty:
            return pd.DataFrame({})
    # Define walking modes/types (adjust based on your specific data categories)
    walking_types = ['walking', 'pedestrian', 'footway', 'path', 'cycleway','sidewalk']
    # Filter the GeoDataFrame to include only walking segments
    gdf = gdf[gdf['class'].isin(walking_types)]
    # gdf = gdf.dissolve()
    # gdf['metric'] = gdf.dissolve().to_crs('EPSG:3857').length[0]

    dissolved = gdf.dissolve()
    dissolved_length = dissolved.to_crs('EPSG:3857').geometry.length[0]
    
    # If dissolved_length is very large and needs normalization
    normalized_length = np.log(dissolved_length)
    
    # Assign this normalized value
    gdf['metric'] = normalized_length
    # gdf = gpd.overlay(gdf, mask, how='intersection')
    gdf = gdf[['geometry','metric']]
    print(gdf)
    return gdf
