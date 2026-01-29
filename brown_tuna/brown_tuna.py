# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None,
       res:int= 11,
       path:str='s3://us-west-2.opendata.source.coop/fiboa/switzerland/switzerland.parquet'):
    # Using common fused functions as helper
    # common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    # tiles = common.get_tiles(bounds, target_num_tiles=16)
    utils = fused.load("https://github.com/fusedio/udfs/tree/e74035a1/public/common/").utils
    bounds = utils.bounds_to_gdf(bounds)
    bounds = bounds.bounds.values[0]
    
    # Buffering tiles internally
    def run_query(path, res, bounds):

        xmin, ymin, xmax, ymax = bounds
        utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
        # Connect to DuckDB
        con = utils.duckdb_connect()
        query=f"""
    with to_cells as ( 
    SELECT
      
      unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'overlap')) as hex,

      crop_name
      from read_parquet('{path}')
    
    where
        geometry >= {ymin}
        AND geometry < {ymax}
        AND geometry >= {xmin}
        AND geometry < {xmax})
    
    select hex as hex, crop_name 
    from to_cells 
    group by all
    --group by all
        """
        return con.sql(query).df()
    df = run_query(path=path, res=res, bounds=bounds)