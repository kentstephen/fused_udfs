# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, res: int = 11):
    import pandas as pd
    from utils import run_query
    
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tiles = common.get_tiles(bounds)
    # Buffering tiles internally
    df = run_query(res,bounds)
    if df is None or df.empty:
        return

    return df
    
    # print(gdf.columns)
    # return gdf
