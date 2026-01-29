@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=10):
    import geopandas as gpd
    import shapely
    import pandas as pd
 
    gdf = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    if len(gdf) < 1:
        return
    gdf = gdf[gdf["level1_category_name"].str.contains("Dining and Drinking", case=False, na=False)]
    # df = df[df["name"].str.contains("Starbucks", case=False, na=False)]
    
    # print(df['level3_category_name'])
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    df_poi = pd.DataFrame(gdf)
    if df_poi is None or df_poi.empty:
        return

    # if df is None or df.empty:
    #     return pd.DataFrame()
    def get_cells(df, resolution):
        con = fused.utils.common.duckdb_connect()
        query= f"""
        select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) as hex,
        h3_cell_to_boundary_wkt(hex) boundary,
        count(1) as cnt
        from df
        
        group by 1
     
        """
        df = con.sql(query).df()
        # df['cnt'] = df['cnt']/10
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))

    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=10)
    if len(gdf_overture) < 1:
        return
    gdf_coffee = get_cells(df=df_poi, resolution=resolution)
    gdf_joined = gdf_overture.sjoin(gdf_coffee)
    print(gdf_joined['cnt'].describe())
    return gdf_joined
