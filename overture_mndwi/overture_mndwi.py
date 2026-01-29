# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds = None, 
       res:int=10):
    from utils import get_over, run_query
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/67a77a4/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tile = common.get_tiles(bounds, target_num_tiles=16)
    # Buffering tiles internally
    df_mndwi = fused.run("fsh_72YTuj0SvQuKJuDGJwlahn", bounds=bounds, res=res)
    
    df_building = get_over(tile, 'building')
    if df_mndwi is None or df_mndwi.empty:
        return
    
    bounds = common.bounds_to_gdf(bounds)
    bounds = bounds.bounds.values[0]
    gdf = run_query(df_mndwi, df_building, res, bounds)
    return gdf
