# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, path: str='s3://us-west-2.opendata.source.coop/fiboa/switzerland/switzerland.parquet'):
    from utils import table_to_tile
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    # tiles = common.get_tiles(bounds, target_num_tiles=16)
    gdf = table_to_tile(table=path,bounds=bounds)

    print(gdf)
    return gdf
    
