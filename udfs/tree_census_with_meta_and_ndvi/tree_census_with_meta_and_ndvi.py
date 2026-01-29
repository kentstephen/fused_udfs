# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, res:int=12, tree_scale:int=20, buffer_multiple: float = 1):
    from utils import read_data, df_to_hex
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/029c301/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tiles = common.get_tiles(bounds)
    # Buffering tiles internally
    df = read_data()
    df_dem = fused.run("fsh_1eOeIAywTEvRrxmjR7kIVr", bounds=bounds, res=res, tree_scale=tree_scale) # ndvi meta trees
    df = df_to_hex(df, df_dem, res)
    return df
    # print(df.columns)