@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=12):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb

    df_poi = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    df_poi = df_poi["Landmarks and"
    # df_poi['geometry'] = df_poi['geometry'].apply(shapely.wkt.dumps)
    # df_poi = pd.DataFrame(df_poi)

    if df_poi is None or df_poi.empty:
         return pd.DataFrame()
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
    # print(df_poi)
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
    
    # @fused.cache
    def get_cells(df_poi, df_kontur, resolution):
        con = fused.utils.common.duckdb_connect()
        query=f""" with to_cells as(
       select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) AS hex,
        
        from df_poi
        group by 1
        ) Select
            p.hex,
            h3_cell_to_boundary_wkt(p.hex) as boundary,
            k.pop
            from to_cells p inner join df_kontur k on h3_cell_to_parent(p.hex, 8) = k.hex

        
        """
        df = con.sql(query).df()
        # print(df)
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))

    df = get_cells(df_poi, df_kontur, resolution)
    print(df)
     # Get Overture Buildings
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=10)
    
    # Join H3 with Buildings using coffe_score to visualize, you can change to a left join
    gdf_joined = gdf_overture.sjoin(df, how="inner", predicate="intersects")

    gdf_joined = gdf_joined.drop(columns='index_right')
    print(gdf_joined)
    # df = add_rgb(df, 'cnt')
    return gdf_joined