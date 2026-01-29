@fused.udf
def udf(bbox: fused.types.TileGDF = None, path: str='s3://fused-users/stephenkentdata/paris_cafe_coffe_10_r_9.parquet'):
    import geopandas as gpd
    import pandas as pd
    import duckdb
    import shapely
    from utils import nyc_mask
    # from utils import add_rgb
    bounds = bbox.bounds.values[0] 
    # con = fused.utils.common.duckdb_connect()
    df_isochrones = duckdb.sql(f"from read_parquet('{path}')").df()
    # df = add_rgb(df, 'cnt')

    def read_data(df):
   
        con = fused.utils.common.duckdb_connect()
        query = f"""
        SELECT hex,
               cnt,
              h3_cell_to_boundary_wkt(hex) as boundary,
              
        FROM df
       
        
  
       -- GROUP BY hex
        """
        df = con.sql(query).df()
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    gdf_h3 = read_data(df=df_isochrones)
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, use_columns=['height'], min_zoom=10)
    # gdf_overture = nyc_mask(gdf_overture)
    # # Join H3 with Buildings using coffe_score to visualize, you can change to a left join
    gdf_joined = gdf_overture.sjoin(gdf_h3, how="inner", predicate="intersects")
    gdf_joined = gdf_joined.drop(columns='index_right')
    
    print(gdf_h3)
    return gdf_joined
    