# @fused.cache
def table_to_tile(bounds):
    import pandas as pd
    import shapely
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    gdf = common.table_to_tile(table='s3://fused-users/stephenkentdata/stephenkentdata/fiboa/USDA/',bounds=bounds, use_columns=['geometry'],min_zoom=0)
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    return pd.DataFrame(gdf)
# @fused.cache
def run_query(res, bounds):
    import pandas as pd
    import geopandas as gpd
    import shapely
    df_fields = table_to_tile(bounds)
    
    df_ndvi = fused.run("fsh_BTA6xFBcix4ROqSNU07et", bounds=bounds, provider="MSPC", time_of_interest="2020-07-15/2020-09-05", res=res)
    if df_fields is None or len(df_fields) == 0 or df_fields.shape[0] == 0:
        
        return None
    if df_ndvi is None or len(df_ndvi) ==0 or df_ndvi.empty:
        return None
    # xmin, ymin, xmax, ymax = 
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""
with fields_to_cells as (
select
 unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'center')) as hex,
df_fields.*
 from df_fields
) 

SELECT
  f.geometry as boundary,
  avg(n.metric) as metric,

  from fields_to_cells f inner join df_ndvi n
 on f.hex=n.hex


group by all
    """
        # Run the query and return as a gdf
    df = con.sql(query).df()
    return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    
# where
#     h3_cell_to_lat(hex) >= {ymin}
#     AND h3_cell_to_lat(hex) < {ymax}
#     AND h3_cell_to_lng(hex) >= {xmin}
#     AND h3_cell_to_lng(hex) < {xmax}
    