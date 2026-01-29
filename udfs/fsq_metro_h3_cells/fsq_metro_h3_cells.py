@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=10):
    import geopandas as gpd
    import shapely
    import pandas as pd

    df_starb = fused.run("fsh_595DZMDqnFaMrFoJJrV886", min_zoom=0, bbox=bbox)
    # print(df['level3_category_name'])
    df_starb['geometry'] = df_starb['geometry'].apply(shapely.wkt.dumps)

    if df_starb is None or df_starb.empty:
         return pd.DataFrame()
    # # Filter for rows where 'level3_category_name' contains "Coffee Shop"
    # df_coffee fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox)
    # df_coffee = df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
    
    # # Exclude rows where 'name' contains "Starbucks"
    # df_coffee = df_coffee[~df_coffee["name"].str.contains("Starbucks", case=False, na=False)]
    # if df_coffee is None or df_coffee.empty:
    #     return pd.DataFrame()
    
    
    def get_cells(df_starb, resolution):
        con = fused.utils.common.duckdb_connect()
        query=f"""
       select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) AS hex,
        count(1) as cnt
        from df_starb
        group by 1

        
        """
        return con.sql(query).df()

    df = get_cells(df_starb, resolution)
    print(df)
    return df