"""
You can use components of this get_iso_gdf as an example of how to generate multiple isochrones using the Get Isochrone UDF.

If you are using a lot of points I suggest running in Jupyter to avoid timeout.
"""
@fused.cache
def get_iso_gdf(costing, time_steps):
   import shapely
   import geopandas as gpd
   import pandas as pd
   # Greater Bushwick
   bbox = gpd.GeoDataFrame(
       geometry=[shapely.box(-73.966036,40.666722,-73.875359,40.726179)], 
       crs=4326
   )
  
   def get_coffee(bbox):
       # Read the GeoJSON file from NYC Open Data
       gdf_tract = gpd.read_file('https://data.cityofnewyork.us/api/geospatial/j2bc-fus8?method=export&format=GeoJSON')
       
       # Filter for rows containing 'Bushwick' dissolve
       gdf_tract = gdf_tract[gdf_tract['ntaname'].str.contains('Bushwick', na=False)].dissolve(by='ntaname').reset_index()
       # Call the FSQ UDF
       df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
       
       if len(df) < 1:
           return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")  # Explicit return for empty case
       # Only want coffee shops
       df = df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
       
       # Apply the mask
       df = gpd.overlay(df, gdf_tract, how='intersection')
       return df
   
   # Get the shops
   df = get_coffee(bbox)
   
   # Function to iterate over Get Isochrone UDF
   def get_batch_isochrone(df, costing, time_steps):
       import geopandas as gpd
       all_isochrones = []
       
       for point in df.geometry:
           try:
               gdf = fused.utils.Get_Isochrone.get_isochrone(
                   lat=point.y,
                   lng=point.x,
                   costing=costing,
                   time_steps=time_steps
               )
               all_isochrones.append(gdf)
           except Exception as e:
               print(f"Error processing point ({point.x}, {point.y}): {str(e)}")
               # Create empty GeoDataFrame with same structure
               empty_gdf = gpd.GeoDataFrame(
                   geometry=[], 
                   crs="EPSG:4326"
               )
               all_isochrones.append(empty_gdf)
               continue
       # Combine the isochrones
       return pd.concat(all_isochrones) if all_isochrones else gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
   # Run the iso function and return
   return get_batch_isochrone(df, costing, time_steps)
    
def get_cells(df_iso, resolution):
    import geopandas as gpd
    import shapely
    # Connect to DuckDB
    con = fused.utils.common.duckdb_connect()
    # Turn the isochromes into cells and then count them
    query=f"""
    with to_cells as (
      select
        unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS hex
      from df_iso
    )
    select 
      hex,
      h3_cell_to_boundary_wkt(hex) as boundary,
      count(*) as coffee_cnt
    from to_cells
    group by hex
    """
    # Run the query and return as a gdf
    df = con.sql(query).df()
    gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    return gdf
        