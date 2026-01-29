import pandas as pd
import shapely
import geopandas as gpd

def get_fsq(bounds):
    # overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    gdf = fused.run("UDF_Foursquare_Open_Source_Places", bounds=bounds, min_zoom=0)
    gdf = gdf[gdf['level2_category_name']=='Hospital']
    if gdf is None or gdf.empty:
        return pd.DataFrame({})
    gdf['lat'] = gdf.geometry.centroid.y
    gdf['lng'] = gdf.geometry.centroid.x

    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    df = pd.DataFrame(gdf)
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query = """select
        h3_latlng_to_cell(lat, lng, 8) as hex,
        name,
        h3_cell_to_lat(hex) as lat,
        h3_cell_to_lng(hex) as lng
        from df
        """
    hex_df = con.sql(query).df()
    
    # Group by hex to reduce duplicate locations
    grouped_df = hex_df.groupby('hex').agg({
        'name': lambda x: ', '.join(set(x)),  # Join unique names
        'lat': 'first',  # Take first lat (all identical within hex)
        'lng': 'first',  # Take first lng (all identical within hex)
    }).reset_index()
    
    # Add count of hospitals per hex
    count_df = hex_df.groupby('hex').size().reset_index(name='hospital_count')
    grouped_df = grouped_df.merge(count_df, on='hex')
    
    print(f"Found {len(hex_df)} hospitals grouped into {len(grouped_df)} H3 cells")
    
    return grouped_df

def get_single_isochrone(point_data):
    # Function for single isochrone
    row, costing, time_steps = point_data
    try:
        get_isochrone_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Get_Isochrone/").utils
        isochrone = get_isochrone_utils.get_isochrone(
            lat=row['lat'],
            lng=row['lng'], 
            costing=costing,
            time_steps=time_steps
        )
        # Add additional attributes from the row to the isochrone
        if len(isochrone) > 0:
            isochrone['name'] = row['name']  # Using 'name' instead of 'hospital_names' for consistency
            isochrone['hospital_count'] = row.get('hospital_count', 1)
            isochrone['hex'] = row['hex']
        return isochrone
    except Exception as e:
        print(f"Error processing point ({row['lat']}, {row['lng']}): {str(e)}")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

def get_pool_isochrones(df, costing, time_steps):
    # Run the isochrone requests concurrently
    if len(df) == 0:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    
    print(f"Making {len(df)} isochrone requests")
    
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Using the Fused common run_pool function 
    arg_list = [(row, costing, time_steps) for _, row in df.iterrows()]
    isochrones = utils.run_pool(get_single_isochrone, arg_list)
    
    # Filter out empty isochrones
    valid_isochrones = [iso for iso in isochrones if len(iso) > 0]
    
    if not valid_isochrones:
        print("No valid isochrones were generated")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    
    print(f"Successfully generated {len(valid_isochrones)} isochrones")
    
    # Combine all isochrones
    result = pd.concat(valid_isochrones)
    
    return result

def get_fsq_isochrones_gdf(costing, time_steps, bounds): 
    # Greater Bushwick
    df = get_fsq(bounds)

    if len(df) == 0:
        print("No hospitals found in the specified bounds")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        
    # Concurrent isochrones  
    return get_pool_isochrones(df, costing, time_steps)


def fsq_isochrones_to_h3(df_fsq_isochrones, resolution, bounds):
    xmin, ymin, xmax, ymax = bounds
    
    # Add the 'name' column if it doesn't exist
    if 'name' not in df_fsq_isochrones.columns:
        df_fsq_isochrones['name'] = 'Hospital'
        
    # Original code continues unchanged
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    con = utils.duckdb_connect()
    query = f"""
    WITH to_cells AS (
     SELECT
      unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS hex,
      name
     FROM df_fsq_isochrones
    )
    SELECT 
     hex,
     h3_cell_to_lat(hex) AS cell_lat,
     h3_cell_to_lng(hex) AS cell_lng,
     count(*) AS poi_density,
     string_agg(DISTINCT name, ', ') AS poi_names
    FROM to_cells
    WHERE cell_lat >= {ymin}
      AND cell_lat < {ymax}
      AND cell_lng >= {xmin}
      AND cell_lng < {xmax}
    GROUP BY hex
    """
    return con.sql(query).df()