@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=10,
     lat: float = 40.71771330421802,  # Note I swapped these to correct lat/lng
    lng: float = -73.95769509630115,
       poi_category: str= None):
    import geopandas as gpd
    import shapely
    from utils import get_isochrones, get_fsq_points, isochrones_to_h3

    gdf_isochrones = get_isochrones(lat, lng)
    print(gdf_isochrones)
    gdf_isochrones = gdf_isochrones['geometry']
    bounds = tuple(gdf_isochrones.total_bounds)  # (minx, miny, maxx, maxy)
    gdf_poi = get_fsq_points(bbox=bounds, poi_category=poi_category)
    print(gdf_poi.columns)
    gdf_poi = gpd.sjoin(gdf_poi, gdf_isochrones.to_frame('geometry'), how='inner', predicate='intersects')
    # gdf = isochrones_to_h3(gdf_isochrones, gdf_poi, resolution)
    # print(gdf)
    return gdf_poi