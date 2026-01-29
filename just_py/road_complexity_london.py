# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, res: int =10):
    from utils import get_over, run_query
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/67a77a4/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tile = common.get_tiles(bounds, target_num_tiles=16)
    # Buffering tiles internally
    gdf_segment = get_over(tile, 'segment')
    # print(gdf_segment)
    if gdf_segment is None or gdf_segment.empty:
        return
    df_connector = get_over(tile, 'connector')
    if df_connector is None or df_connector.empty:
        return
    bounds = common.bounds_to_gdf(bounds)
    bounds = bounds.bounds.values[0]
    gdf= run_query(df_connector, res, bounds)
    gdf_joined = gdf_segment.sjoin(gdf, how='inner', predicate='intersects')
    return gdf_joined