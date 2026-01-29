@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=9):
    import geopandas as gpd
    import shapely
    import pandas as pd
    gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=0)
    if len(gdf) < 1:
        return
    gdf["area_m2"] = gdf.to_crs(gdf.estimate_utm_crs()).area.round(2)
    
    # gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=0)
    if gdf is None or gdf.empty:
        return 
    # gdf = gdf[gdf['class'] == 'hospital']
    if gdf is None or gdf.empty:
        return 
    # gdf['volume_m3'] = gdf['area_m2'] * gdf['height']

    gdf['geometry'] = gdf['geometry'].apply(lambda x: shapely.wkt.dumps(x))
    df_buildings = pd.DataFrame(gdf)
    
        
    
    def get_cells(df, resolution):
        import geopandas
        import shapely
        utils = fused.load(
            "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
        ).utils
        # I am including syntax that you can uncomment in order to join with vector data.
    
        qr = f"""
                select
               h3_latlng_to_cell(ST_Y(ST_Centroid(ST_GeomFromText(geometry))), ST_X(ST_Centroid(ST_GeomFromText(geometry))), {resolution}) as hex,
          --  h3_cell_to_boundary_wkt(hex) boundary, 
                sum(area_m2) as metric
                from df
           
                group by 1
            """
        con = utils.duckdb_connect()
        df = con.query(qr).df()
        # gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
        return df
        # return df
    df_buildings = get_cells(df_buildings, resolution)
    # df_places = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox)
    # gdf_places = gpd.GeoDataFrame(
    #     df_places,
    #     geometry="geometry",
    #     crs="EPSG:4326"  # Original CRS in WGS 84
    # )
    # gdf_joined = gdf_places.sjoin(gdf_buildings)
    # # print(gdf_buildings)
    # # return gdf_joined
    # gdf_mercator = gdf_joined.to_crs(epsg=3857)

    # # Apply the buffer in meters
    # gdf_mercator['geometry'] = gdf_mercator.geometry.buffer(50)  # 500 meters buffer
    
    # Reproject back to the original CRS (EPSG:4326)
    # gdf_buffer = gdf_mercator.to_crs(epsg=4326)
    print(df_buildings)
    return df_buildings