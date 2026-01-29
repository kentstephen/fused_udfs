def udf(bbox: fused.types.TileGDF = None, release: str = "2024-08-20-0", use_columns: list = None, num_parts: int = None, min_zoom: int = 0, polygon: str = None, point_convert: str = None, 
        buffer_distance: int = 1000):
    from utils import get_overture
    import geopandas as gpd
    
    # Get water data and buffer it
    water_gdf = get_overture(bbox=bbox, release=release, theme="base", overture_type="water", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    water_gdf = water_gdf.to_crs(epsg=3857)
    water_gdf['geometry'] = water_gdf.geometry.buffer(buffer_distance)
    water_gdf = water_gdf.to_crs(epsg=4326)
    
    # Get industrial land use areas
    land_use_gdf = get_overture(bbox=bbox, release=release, theme="base", overture_type="land_use", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    industrial_land_use = land_use_gdf[land_use_gdf['class'] == 'industrial']
    
    # Find industrial land use areas near water
    industrial_near_water = gpd.sjoin(industrial_land_use, water_gdf, how="inner", predicate="intersects")
    industrial_near_water = industrial_near_water[['geometry']]
    
    # Get all buildings
    building_gdf = get_overture(bbox=bbox, release=release, theme="buildings", overture_type="building", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    
    # Find buildings within industrial areas near water
    gdf_joined = gpd.sjoin(building_gdf[['geometry', 'num_floors', 'subtype']], industrial_near_water, how="inner", predicate="intersects")
    
    print(f"gdf_joined columns: {gdf_joined.columns}")
    print(gdf_joined["subtype"])
    return gdf_joined