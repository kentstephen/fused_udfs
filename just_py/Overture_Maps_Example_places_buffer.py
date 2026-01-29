def udf(bbox: fused.types.TileGDF = None, release: str = "2024-08-20-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = 0, polygon: str = None, point_convert: str = None,
       buffer_distance: int = None, buffer_place: str = None ):
    from utils import get_overture
    import geopandas as gpd
    
    # Get places data and filter for coffee shops
    places_gdf = get_overture(bbox=bbox, release=release, theme="places", overture_type="place", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    mask = places_gdf['categories'].apply(lambda x: isinstance(x, dict) and x.get('primary') == buffer_place)
    gdf = places_gdf[mask]
    gdf = gdf.to_crs(epsg=3857)
        # Apply the buffer
    gdf['geometry'] = gdf['geometry'].buffer(buffer_distance)
        # Convert back to EPSG:4326
    gdf = gdf.to_crs("EPSG:4326")
    # Get building data
    building_gdf = get_overture(bbox=bbox, release=release, theme="buildings", overture_type="building", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    
    # Perform spatial join to get coffee shop buildings
    coffee_shop_buildings = gpd.sjoin(building_gdf, gdf, how="inner", predicate="intersects")
    return coffee_shop_buildings