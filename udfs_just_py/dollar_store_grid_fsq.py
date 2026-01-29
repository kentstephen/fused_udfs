@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int=8,
       disk_size: int= 20):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb
    
    def get_dollar(bbox):
        df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
        if len(df) < 1:
            return
        # Convert geometry first, before any filtering
        df['geometry'] = df['geometry'].apply(lambda x: shapely.wkt.dumps(x))
        df = pd.DataFrame(df)
        # Then do filtering
        df = df[df["level2_category_name"].str.contains("Discount Store", case=False, na=False)]
        df = df[df["name"].str.contains("Dollar", case=False, na=False)]
        # df['geometry'] = df['geometry'].apply(lambda x: shapely.wkt.dumps(x))
        print(df['geometry'].dtype)
        return df
    
    def get_grocery(bbox):
        df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
        if len(df) < 1:
            return
        # Convert geometry first, before any filtering
        df['geometry'] = df['geometry'].apply(lambda x: shapely.wkt.dumps(x))
        df = pd.DataFrame(df)
        # Then do filtering
        df = df[df["level3_category_name"].str.contains("grocery store", case=False, na=False)]
        # df['geometry'] = df['geometry'].apply(lambda x: shapely.wkt.dumps(x))
        print(df['geometry'].dtype)
        return df
    
    df = get_dollar(bbox)
    df_grocery = get_grocery(bbox)
    def get_cells(resolution, df, df_grocery):
        con = fused.utils.common.duckdb_connect()
        query= f"""
        with dollar_cells as (
        select
            h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) as hex,
        count(1) as cnt
        from df

        group by 1
), grocery_cells as 
( 
select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) as hex,
        count(1) as cnt
    from df_grocery
    group by 1),
     to_disk as (
    select 
        unnest(h3_grid_disk(hex, {disk_size})) as disk_hex,
        hex as center_hex,
        cnt
    from dollar_cells )
    
        select
        t.center_hex as hex,
        SUM(t.cnt) as cnt,
        SUM(coalesce(g.cnt, 0)) as comp_cnt
        from to_disk t
        left join grocery_cells g on t.disk_hex=g.hex
       group by 1


    
        """
        df = con.sql(query).df()
        return df
    df_final = get_cells(resolution, df, df_grocery)
    # df_final = add_rgb(df_final, 'cnt')
    print(df_final)
    # print(df_grocery)
    # return df_grocery
    return df_final