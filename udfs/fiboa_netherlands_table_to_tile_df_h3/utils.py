@fused.cache
def table_to_tile(bounds):
    import pandas as pd
    import shapely
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    gdf = common.table_to_tile(table='s3://fused-users/stephenkentdata/stephenkentdata/fiboa/netherlands_crops/',bounds=bounds, use_columns=['geometry', 'crop_name','id'],min_zoom=0)
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    return pd.DataFrame(gdf)
@fused.cache
def run_query(res, bounds):
    df = table_to_tile(bounds)
    xmin, ymin, xmax, ymax = bounds
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""
with fields_to_cells as (
select
 unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'center')) as hex,
 crop_name,
 id
 from df
) 

SELECT
  hex,
  id,
  crop_name
 
  from fields_to_cells 

where
    h3_cell_to_lat(hex) >= {ymin}
    AND h3_cell_to_lat(hex) < {ymax}
    AND h3_cell_to_lng(hex) >= {xmin}
    AND h3_cell_to_lng(hex) < {xmax}
group by all
    """
    return con.sql(query).df()
    