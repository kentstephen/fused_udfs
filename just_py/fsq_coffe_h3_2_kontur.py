@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=8):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb

    df_poi = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    df_poi = df_poi[df_poi["level3_category_name"]=="Coffee Shop"]

    print(f"df_poi type{type(df_poi)}")
    # Load the boroughs GeoJSON file
    # boroughs_gdf = gpd.read_file("https://data.cityofnewyork.us/api/geospatial/tqmj-j8zm?method=export&format=GeoJSON", driver="GeoJSON")

    # Load the boroughs GeoJSON file
    boroughs_gdf = gpd.read_file("https://data.cityofnewyork.us/api/geospatial/tqmj-j8zm?method=export&format=GeoJSON", driver="GeoJSON")
    
    # Combine all boroughs into a single geometry using union_all
    dissolved_geometry = boroughs_gdf.geometry.union_all()
    
    # Wrap the dissolved geometry in a GeoDataFrame
    dissolved_gdf = gpd.GeoDataFrame({'geometry': [dissolved_geometry]}, crs=boroughs_gdf.crs)
    
    # Ensure CRS is aligned with df_poi
    if df_poi.crs != dissolved_gdf.crs:
        dissolved_gdf = dissolved_gdf.to_crs(df_poi.crs)
    
    # Perform an overlay between df_poi and the dissolved geometry
    df_poi = gpd.overlay(df_poi, dissolved_gdf, how='intersection')
    df_poi['geometry'] = df_poi['geometry'].apply(shapely.wkt.dumps)
    df_poi = pd.DataFrame(df_poi)
    if df_poi is None or df_poi.empty:
         return pd.DataFrame()
    df_kontur = fused.run("fsh_1Kg290kb0BPNahbrby4Kah", bbox=bbox)
    # # Filter for rows where 'level3_category_name' contains "Coffee Shop"
    # df_coffee fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox)
    # df_coffee = df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
    
    # # Exclude rows where 'name' contains "Starbucks"
    # df_coffee = df_coffee[~df_coffee["name"].str.contains("Starbucks", case=False, na=False)]
    # if df_coffee is None or df_coffee.empty:
    #     return pd.DataFrame()
    
    
    def get_cells(df_poi, df_kontur, resolution):
        con = fused.utils.common.duckdb_connect()
        query=f"""with place_cnt as(
       select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) AS hex,
        count(1) as coffee_cnt
        from df_poi
        group by 1)
        SELECT
        h3_h3_to_string(p.hex) as hex,
        p.coffee_cnt,
        k.pop
        from place_cnt p INNER join df_kontur k on p.hex = k.hex
        

        
        """
        return con.sql(query).df()

    df = get_cells(df_poi, df_kontur, resolution)
    print(df)
    # df = add_rgb(df, 'cnt')
    return df