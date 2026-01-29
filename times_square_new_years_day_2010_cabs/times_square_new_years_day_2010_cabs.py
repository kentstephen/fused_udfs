@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=13):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb
    import duckdb
    @fused.cache
    def get_data():
        df_lines = duckdb.sql("from read_parquet('s3://fused-users/stephenkentdata/times_square_yellow_routes.parquet')").df()
        # print(df_lines)
        # print(df_lines.columns)
        df_lines['geometry'] = df_lines['geometry'].apply(shapely.wkt.loads)
        gdf = gpd.GeoDataFrame(df_lines, geometry=df_lines.geometry, crs='EPSG:4326')
        # print(gdf)
        gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(5).to_crs('EPSG:4326')
        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)
    df_routes = get_data()
    print(df_routes)
    @fused.cache
    def get_cells(df_routes, resoultion):
        con = fused.utils.common.duckdb_connect()
        # Convert the isochrones into H3, count the overlap and keep the POI name
        query = f"""
        with to_cells as (
         select
          unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS hex
         from df_routes
      --   limit 2
        )
        select 
         hex,
         count(*) as cnt
        from to_cells
        group by hex
        """
        # Run the query and return a GeoDataFrame
        return con.sql(query).df()
    df = get_cells(df_routes, resolution)
    print(df)
    df = add_rgb(df, 'cnt')
    return df
    
    # return gdf
