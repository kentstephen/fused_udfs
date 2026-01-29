def get_isochrones(lat, lng):  # Added missing parameters
        import geopandas as gpd
        try:
            gdf = fused.utils.Get_Isochrone.get_isochrone(
                lat=lat,
                lng=lng,
                costing="pedestrian",
                time_steps=[15]
            )
            return gdf
            
        except Exception as e:
            print(f"Error processing point: {str(e)}")  # Simplified error message since x,y not defined
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

def get_fsq_points(bbox, poi_category):
    import geopandas as gpd
    # Pull the points
    df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    # Check if the df is empty
    if len(df) < 1:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    # Define the dictionary for filtering
    category_dict = {
        None: df,
        "Bar": df[df["level2_category_name"].str.contains("Bar", case=False, na=False)],
        "Coffee Shop": df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)],
        "Grocery Store": df[df["level3_category_name"].str.contains("Grocery Store", case=False, na=False)],
        "Restaurant" : df[df["level2_category_name"].str.contains("Restaurant", case=False, na=False)],
        "Pharmacy" : df[df["level2_category_name"] == "Pharmacy"],
    }
    
    # Return the filtered DataFrame based on POI category
    return category_dict.get(poi_category, gpd.GeoDataFrame(geometry=[], crs="EPSG:4326"))
def wkt_and_pd(gdf):
    import pandas as pd
    import shapely
       # Converts geometries to WKT for DuckDB 
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    return pd.DataFrame(gdf)
    
def isochrones_to_h3(gdf_isochrones, gdf_poi, resolution):
    import geopandas as gpd
    import shapely
    # Connect to DuckDB
    df_isochrones = wkt_and_pd(gdf=gdf_isochrones)
    df_poi = wkt_and_pd(gdf=gdf_poi)
    con = fused.utils.common.duckdb_connect()
    # Convert the isochrones into H3, count the overlap and keep the POI name
    query = f"""
    with to_cells as (
    select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) AS hex,
        string_agg(DISTINCT name, ', ') as poi_names,
        count(1) as cnt
        from df_poi
        group by 1
    ),
     polyfill as (
     select
      unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS hex
     from df_isochrones
    )
    select 
     h3_h3_to_string(t.hex) as hex,
     --h3_cell_to_boundary_wkt(t.hex) as boundary,
     t.cnt,
     t.poi_names     
    from to_cells t inner join polyfill p on t.hex = p.hex
   -- group by t.hex
    """
    # Run the query and return a GeoDataFrame
    return con.sql(query).df()
    # return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
