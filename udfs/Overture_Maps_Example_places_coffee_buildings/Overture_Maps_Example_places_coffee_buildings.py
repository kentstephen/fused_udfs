def udf(bbox: fused.types.TileGDF = None, release: str = "2024-08-20-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = None, polygon: str = None, point_convert: str = None,
       buffer_distance: int=200):
    from utils import get_overture, get_route_gdf
    import geopandas as gpd

    # Get places data and filter for coffee shops
    places_gdf = get_overture(bbox=bbox, release=release, theme="places", overture_type="place", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    mask = places_gdf['categories'].apply(lambda x: isinstance(x, dict) and x.get('primary') == 'coffee_shop')
    coffee_shops = places_gdf[mask]
    # print("Coffee shops:", len(coffee_shops))

    # Get building data
    building_gdf = get_overture(bbox=bbox, release=release, theme="buildings", overture_type="building", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    # print("Buildings:", len(building_gdf))

    # Perform spatial join to get coffee shop buildings
    coffee_shop_buildings = gpd.sjoin(building_gdf, coffee_shops, how="inner", predicate="contains")
    print("Coffee shop buildings:", len(coffee_shop_buildings))

    # Get route data and create buffer
    route_gdf = get_route_gdf()
    route_line = route_gdf[route_gdf.geometry.type == 'LineString']
    route_line = route_line.to_crs(epsg=3857)  # Convert to a projected CRS
    route_buffer = route_line.copy()
    route_buffer['geometry'] = route_buffer.geometry.buffer(buffer_distance)
    route_buffer = route_buffer.to_crs(epsg=4326)

    # Ensure coffee_shop_buildings is in the same CRS as route_buffer
    coffee_shop_buildings = coffee_shop_buildings.to_crs(route_buffer.crs)

    columns_to_drop = ['index_left', 'index_right']
    coffee_shop_buildings = coffee_shop_buildings.drop(columns=columns_to_drop, errors='ignore')
    # route_buffer = route_buffer.drop(columns=columns_to_drop, errors='ignore')

# Now perform the spatial join
    gdf_joined = coffee_shop_buildings.sjoin(route_buffer)
    
    # print("Coffee shop buildings near route:", len(coffee_shops_near_route))
    print(route_buffer)
    return gdf_joined