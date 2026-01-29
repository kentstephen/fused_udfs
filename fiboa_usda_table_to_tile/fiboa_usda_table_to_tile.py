# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, table_path:str='gs://fused-fd-stephenkentdata/stephenkentdata/stephenkentdata/fiboa/USDA_NationalCSB_2017-2024_rev23/'):
    import pandas as pd
    # from utils import table_to_tile
    # Using common fused functions as helper
    common =fused.load("https://github.com/fusedio/udfs/tree/e947092/public/common/")

    # This helper function turns our bounds into XYZ tiles
    tile = common.get_tiles(bounds)
    # Buffering tiles internally
    gdf = table_to_tile(table_path, tile)
    # df_fiboa = pd.DataFrame(gdf)
    print(gdf.columns)
    return gdf
def table_to_tile(table_path, tile):
    
    common = fused.load("https://github.com/fusedio/udfs/tree/e947092/public/common/")
    return common.table_to_tile(table=table_path, bounds=tile, min_zoom=0)
    