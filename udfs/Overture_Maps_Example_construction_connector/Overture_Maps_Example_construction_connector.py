def udf(bbox: fused.types.TileGDF = None, release: str = "2024-08-20-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = 0, polygon: str = None, point_convert: str = None,
        buffer_place: str = None ):
    from utils import get_overture
    import geopandas as gpd
    
    # Get places data and filter for coffee shops
    gdf = get_overture(bbox=bbox, release=release, theme="base", overture_type="land_use", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    gdf = gdf[gdf['subtype'] == 'construction']
   
    gdf = gdf.to_crs(epsg=3857)
    gdf["buffer_distance"] = gdf.geometry.area  / 100
        # Apply the buffer
    gdf['geometry'] = gdf['geometry'].buffer(gdf["buffer_distance"])
    print(gdf["buffer_distance"])
    # print(gdf.buffer_distance.dtype)
        # Convert back to EPSG:4326
    gdf = gdf.to_crs("EPSG:4326")
    # Get building data
    connector_gdf = get_overture(bbox=bbox, release=release, theme="transportation", overture_type="connector", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    
    # Perform spatial join to get coffee shop buildings
    gdf_joined = gpd.sjoin(connector_gdf, gdf, how="inner", predicate="intersects")
    return gdf_joined