@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=12):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb

    df_poi = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    df_poi = df_poi[
    (df_poi['level3_category_name'] == "Deli") |
    (df_poi['level2_category_name'] == "Convenience Store")
    ]
    df_poi['geometry'] = df_poi['geometry'].apply(shapely.wkt.dumps)
    df_poi = pd.DataFrame(df_poi)

    if df_poi is None or df_poi.empty:
         return pd.DataFrame()
    # # Filter for rows where 'level3_category_name' contains "Coffee Shop"
    # df_coffee fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox)
    # df_coffee = df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
    
    # # Exclude rows where 'name' contains "Starbucks"
    # df_coffee = df_coffee[~df_coffee["name"].str.contains("Starbucks", case=False, na=False)]
    # if df_coffee is None or df_coffee.empty:
    #     return pd.DataFrame()
    
    
    def get_cells(df_poi, resolution):
        con = fused.utils.common.duckdb_connect()
        query=f"""
       select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) AS hex,
        
        from df_poi
        group by 1

        
        """
        df = con.sql(query).df()
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))

    df = get_cells(df_poi, resolution)
     # Get Overture Buildings
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, use_columns=use_columns, min_zoom=10)
    
    # Join H3 with Buildings using coffe_score to visualize, you can change to a left join
    gdf_joined = gdf_overture.sjoin(df, how="inner", predicate="intersects")

    gdf_joined = gdf_joined.drop(columns='index_right')
    
    # df = add_rgb(df, 'cnt')
    return df