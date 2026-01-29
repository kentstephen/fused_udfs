# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds = None, 
        time_of_interest="2025-07-15/2025-08-15",
        res: int = 11):
    import pandas as pd
    from utils import table_to_tile, run_query
    
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tile = common.get_tiles(bounds)
    # Buffering tiles internally
    # df_fields = table_to_tile(bounds)
    # df = run_query(res,bounds)
    # df_ndvi = fused.run("fsh_1W8TPglX6f6KuaRxSM0J5l", bounds=bounds, res=res)
    # if df_ndvi is None or df_ndvi.empty:
    #     return
    gdf = run_query(res, bounds, time_of_interest)
    if gdf is None or gdf.empty:
        return
    if gdf['avg_ndvi'] is not None:
        gdf['avg_ndvi']= round(gdf['avg_ndvi']-5, 2)
    print(gdf[['neighborhood', 'avg_ndvi']].sort_values(by=['avg_ndvi']))
    return gdf
    
    # print(gdf.columns)
    # return gdf
