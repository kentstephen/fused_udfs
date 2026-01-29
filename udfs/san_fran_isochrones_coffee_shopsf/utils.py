"""
You can use components of this get_iso_gdf as an example of how to generate multiple isochrones using the Get Isochrone UDF.

If you are using a lot of points I suggest running in Jupyter to avoid timeout.
"""
@fused.cache
def get_iso_gdf(costing, time_steps):
    import shapely
    import geopandas as gpd
    import pandas as pd
   
    bbox = gpd.GeoDataFrame(
        geometry=[shapely.box(-122.5288,37.677787,-122.353018,37.815913)], 
        crs=4326
    )
    def get_coffee(bbox):
        # gdf_tract = gpd.read_file('https://data.cityofnewyork.us/api/geospatial/j2bc-fus8?method=export&format=GeoJSON')
        # gdf_tract = gdf_tract.dissolve(by='ntaname').reset_index()
        df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
       
        if len(df) < 1:
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
           
        df = df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
        # df = gpd.overlay(df, gdf_tract, how='intersection')
        return df
    @fused.cache
    def get_single_isochrone(point_data):
        point, costing, time_steps = point_data
        try:
            gdf = fused.utils.Get_Isochrone.get_isochrone(
                lat=point.y,
                lng=point.x, 
                costing=costing,
                time_steps=time_steps
            )
            return gdf
        except Exception as e:
            print(f"Error processing point ({point.x}, {point.y}): {str(e)}")
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    def get_pool_isochrone(df, costing, time_steps):
        arg_list = [(point, costing, time_steps) for point in df.geometry]
        all_isochrones = fused.utils.common.run_pool(get_single_isochrone, arg_list)
        return pd.concat(all_isochrones) 

    df = get_coffee(bbox)
    print(df)
    if len(df) < 1:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
       
    gdf = get_pool_isochrone(df, costing, time_steps)
    print(gdf)
    return gdf
   
    
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
        