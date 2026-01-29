import shapely
import pandas as pd
import geopandas as gpd
def get_fsq(bounds, place_type):
        overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
        gdf = fused.run("UDF_Foursquare_Open_Source_Places", bounds=bounds, min_zoom=0)
        
        gdf['lat'] = gdf.geometry.centroid.y
        gdf['lng'] = gdf.geometry.centroid.x

        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        category_dict = {
        "None": gdf,
        "Bar": gdf[gdf["level2_category_name"].str.contains("Bar", case=False, na=False)],
        "Coffee Shop": gdf[gdf["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)],
        "Grocery Store": gdf[gdf["level3_category_name"].str.contains("Grocery Store", case=False, na=False)],
        "Restaurant" : gdf[gdf["level2_category_name"].str.contains("Restaurant", case=False, na=False)],
        "Pharmacy" : gdf[gdf["level2_category_name"].str.contains("Pharmacy", case=False, na=False)],
        }
    
        # Return the filtered DataFrame based on point_type
        return category_dict.get(place_type, gpd.GeoDataFrame(geometry=[], crs="EPSG:4326"))
        if gdf is None or gdf.empty:
            return pd.DataFrame({})
        return pd.DataFrame(gdf)
def join_h3_buildings_with_fsq(df_buildings, df_fsq, resolution):
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    # Convert the isochrones into H3, count the overlap and keep the POI name
    query = f"""
SELECT
  b.id,
  b.hex,
  b.height,
  COALESCE(f.name, NULL) as name,
  COALESCE(f.geometry, NULL) as boundary,
  CASE 
    WHEN f.level1_category_name IS NULL THEN NULL 
    WHEN f.level1_category_name = 'None' THEN NULL 
    ELSE f.level1_category_name 
  END as level1_category_name,
  COALESCE(f.level2_category_name, NULL) as level2_category_name,
  COALESCE(f.level3_category_name, NULL) as level3_category_name,
  COALESCE(f.fsq_category_ids, NULL) as fsq_category_ids
FROM df_buildings b 
left JOIN df_fsq f
  ON b.hex = h3_latlng_to_cell(f.lat, f.lng, {resolution})

--from df_buildings b left join df_fsq f
--on b.hex = h3_latlng_to_cell(f.lat, f.lng, {resolution})
    """
    # Run the query and return a GeoDataFrame
    return con.sql(query).df()
    # return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
def add_rgb_cmap(gdf, key_field, cmap_dict):
    import pandas as pd
    def get_rgb(value):
        if pd.isna(value):
            return [128, 128, 128]  # Special color for NaN values
        return cmap_dict.get(value)  # Get color from the map
    
    rgb_series = gdf[key_field].apply(get_rgb)
    
    # Set RGB values for all rows
    gdf['r'] = rgb_series.apply(lambda x: x[0])
    gdf['g'] = rgb_series.apply(lambda x: x[1])
    gdf['b'] = rgb_series.apply(lambda x: x[2])
    
    # Only set alpha for NaN values
    nan_mask = gdf[key_field].isna()
    gdf.loc[nan_mask, 'a'] = 1  # Low opacity only for NaN values
    
    return gdf
CMAP = {
    "Dining and Drinking": [101, 45, 144],
    "Business and Professional Services": [53, 151, 103],
    "Retail": [55, 96, 146],
    "NA": [248, 196, 49],
    "Travel and Transportation": [235, 83, 144],
    "Community and Government": [146, 200, 100],
    "Arts and Entertainment": [233, 131, 40],
    "Landmarks and Outdoors": [39, 153, 171],
    "Health and Medicine": [189, 61, 146],
    "Sports and Recreation": [238, 156, 146],
    "Event": [82, 85, 156]
}