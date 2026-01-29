@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=10):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb
    df_places = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    df_places['geometry'] = df_places['geometry'].apply(shapely.wkt.dumps)
    if df_places is None or df_places.empty:
        return pd.DataFrame()
    def get_cells(df_places, resolution):
        con = fused.utils.common.duckdb_connect()
        query= f"""
        select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) as hex,
        h3_cell_to_boundary_wkt(hex) boundary,
        count(1) as cnt
        from df_places
        where level1_category_name  = 'Dining and Drinking'
        group by 1
        order by cnt desc
        """
        df = con.sql(query).df()
        gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
        return gdf
    gdf_places = get_cells(df_places,resolution)
    gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=10)
    if gdf is None or gdf.empty:
        return pd.DataFrame()
    gdf_joined = gdf.sjoin(gdf_places, how="left")
    print(gdf_joined)
    # df = add_rgb(df, 'cnt')
    return gdf_joined
    
    