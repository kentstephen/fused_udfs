@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=12):
    import geopandas as gpd
    import shapely
    import pandas as pd

    # df_starb = fused.run("fsh_1u3O9w2QBQFZCW6HVQpTEi", bbox=bbox)
    # # print(df['level3_category_name'])
    # df_starb['geometry'] = df_starb['geometry'].apply(shapely.wkt.dumps)

    # if df_starb is None or df_starb.empty:
    #      return pd.DataFrame()
    # Filter for rows where 'level3_category_name' contains "Coffee Shop"
    df_coffee = fused.run("UDF_Foursquare_Open_Source_Places", min_zoom=0, bbox=bbox)
    df_coffee = df_coffee[df_coffee["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
    
    # Exclude rows where 'name' contains "Starbucks"
    # df_coffee = df_coffee[~df_coffee["name"].str.contains("Starbucks", case=False, na=False)]
    df_coffee['geometry'] = df_coffee['geometry'].apply(shapely.wkt.dumps)
    if df_coffee is None or df_coffee.empty:
        return 
    
    df_route = fused.run("fsh_1AnVgRSpFoJXHPlolb65oM")
    def get_cells(df_coffee, df_route, resolution):
        con = fused.utils.common.duckdb_connect()
        query=f"""
        with to_cells as (
       select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) AS hex,
        string_agg(DISTINCT name, ', ') as names
        from df_coffee
        group by 1
        )
        select
        t.hex,
        t.names,
        r.route_overlap
        from to_cells t inner join df_route r on t.hex = h3_cell_to_parent(r.hex, {resolution})
        

        
        """
        return con.sql(query).df()

    df = get_cells(df_coffee, df_route, resolution)
    print(df)
    return df