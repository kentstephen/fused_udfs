import pandas as pd
import shapely
import geopandas as gpd
def get_single_isochrone(point_data):
    # Function for single isochrone
    point, costing, time_steps = point_data
    try:
        return fused.utils.Get_Isochrone.get_isochrone(
            lat=point.y,
            lng=point.x, 
            costing=costing,
            time_steps=time_steps
        )
    except Exception as e:
        print(f"Error processing point ({point.x}, {point.y}): {str(e)}")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        
# @fused.cache
def get_pool_isochrones(df, costing, time_steps):
    # Run the isochrone requests concurrently
    if len(df) == 0:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    # Using the Fused common run_pool function 
    arg_list = [(point, costing, time_steps) for point in df.geometry]
    isochrones = fused.utils.common.run_pool(get_single_isochrone, arg_list)
    
    # Track which isochrones are valid along with their names
    valid_pairs = [(iso, name) for iso, name in zip(isochrones, df['name']) if len(iso) > 0]
    if not valid_pairs:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    
    # Unzip the pairs and add names to each isochrone in the pair
    valid_isochrones, names = zip(*valid_pairs)
    result = pd.concat(valid_isochrones)
    
    # Add names by repeating each name for its corresponding isochrone's rows
    name_list = []
    for iso, name in zip(valid_isochrones, names):
        name_list.extend([name] * len(iso))
    result['name'] = name_list
    
    return result
def get_fsq_isochrones_gdf(df, costing, time_steps): 
    # Greater Bushwick
    bbox = gpd.GeoDataFrame(
       geometry=[shapely.box(-73.966036,40.666722,-73.875359,40.726179)], 
       crs=4326
    )
    # Coffee shops
    # df = get_fsq_points(bbox, poi_category)

    # if len(df) == 0:
    #     return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        
    # Concurrent isochrones  
    return get_pool_isochrones(df, costing, time_steps)