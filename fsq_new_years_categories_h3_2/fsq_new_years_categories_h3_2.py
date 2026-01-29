@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=13):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb_cmap, CMAP
    # Shared token: UDF_Foursquare_Open_Source_Places
    gdf = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    if gdf.crs != 'EPSG:4326':
        gdf.to_crs('EPGS:4326')
    if len(gdf) < 1:
        return
    @fused.cache
    def get_categories(gdf):
        # Filter out rows where 'name' is empty or NaN
        # gdf = gdf[gdf['name'].notna() & (gdf['name'] != '')]
    
        if gdf.empty:
            return gdf  # Return unchanged GeoDataFrame if filtered result is empty
    
        # Define the key phrases
        cats = [
            "Arts and Entertainment",
            "Landmarks and Outdoors",
             "Travel and Transportation",
            "Sports and Recreation",   
            "Health and Medicine"
        ]
        
        # Filter the GeoDataFrame for rows where 'level1_category_name' matches any of the key phrases
        gdf = gdf[gdf['level1_category_name'].isin(cats)]
        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)
        




        
      


    df_poi = get_categories(gdf)
    if df_poi is None or df_poi.empty:
        return
    @fused.cache
    def get_cells(df_poi, resolution):
        con = fused.utils.common.duckdb_connect()
        query=f"""
       WITH ranked_categories AS (
  SELECT 
    h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) AS hex,
    level1_category_name as cat,
    count(1) as cnt,
    ROW_NUMBER() OVER (PARTITION BY hex ORDER BY count(1) DESC) as rn
  FROM df_poi
  GROUP BY hex, cat
  having cnt >=1
)
SELECT 
  hex,
 -- h3_cell_to_boundary_wkt(hex) as boundary,
  cat as top_cat,
  cnt
FROM ranked_categories
WHERE rn = 1;

        
        """
        return con.sql(query).df()
        # return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
        # return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    df = get_cells(df_poi, resolution)
    df= add_rgb_cmap(df, 'top_cat', CMAP)
    # gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, use_columns=['height'], min_zoom=10)
    # if len(gdf_overture) < 1:
    #     return
    # # Join H3 with Buildings using coffe_score to visualize, you can change to a left join
    # gdf_joined = gdf_overture.sjoin(df, how="inner", predicate="intersects")
    # gdf_joined = gdf_joined.drop(columns='index_right')
    # gdf_joined = add_rgb_cmap(gdf_joined, 'top_cat', CMAP)
  
    # return gdf_joined
    return df
 