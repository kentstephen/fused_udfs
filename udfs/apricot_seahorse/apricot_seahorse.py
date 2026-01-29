# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, buffer_multiple: float = 1,
        path: str='s3://fused-users/stephenkentdata/2025/whitemountains/wmnf_boundary.zip'):
    import geopandas as gpd
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/cd73dbd/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tiles = common.get_tiles(bounds, target_num_tiles=16)
    # Buffering tiles internally
    gdf = gpd.read_file(path)
    
    gdf = gdf.dissolve().to_crs(4326)
    print(gdf.columns)
    # print(gdf['REGIONNAME'])
    # gdf = gdf[gdf['REGIONNAME'].str.contains("hampshire", case=False, na=False)]
    print(gdf)
    return gdf