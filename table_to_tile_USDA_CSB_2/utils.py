def table_to_tile(bounds):
    import pandas as pd
    import shapely
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    gdf = common.table_to_tile(table='s3://fused-users/stephenkentdata/stephenkentdata/fiboa/USDA/',bounds=bounds, use_columns=['geometry'],min_zoom=0)
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    return pd.DataFrame(gdf)
def run_query(res, bounds):
    import pandas as pd
    import geopandas as gpd
    import shapely
    df_fields = table_to_tile(bounds)
    
    df_cdl_hex = fused.run("fsh_1YA2Qj5GncNwOycnfa5Idr", bounds=bounds, res=res)
    if df_fields is None or len(df_fields) == 0 or df_fields.shape[0] == 0:
        
        return None
    if df_cdl_hex is None or len(df_cdl_hex) ==0 or df_cdl_hex.empty:
        return None
    # xmin, ymin, xmax, ymax = 
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""
WITH fields_to_cells AS (
  SELECT
    unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res} , 'center')) as hex, -- for res from df_cdl_hex
    geometry
 
  FROM df_fields
),
field_crop_summary AS (
  SELECT 
   
    """
        # Run the query and return as a gdf
    df = con.sql(query).df()
    return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    