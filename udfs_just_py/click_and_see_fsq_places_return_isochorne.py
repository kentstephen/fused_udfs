@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=10,
     lat: float = 40.71771330421802,  # Note I swapped these to correct lat/lng
    lng: float = -73.95769509630115,
       poi_category: str= None,
       costing: str='pedestrian',
       time_step: int= 15,
       return_isochrones: bool= False):
    import geopandas as gpd
    import shapely
    import json
    from utils import get_isochrones, get_fsq_points, isochrones_to_h3
    
    gdf_isochrones = get_isochrones(lat, lng, costing, time_steps=[time_step])
    gdf_isochrones = gdf_isochrones['geometry']
    print(type(gdf_isochrones))
    # bounds = tuple(gdf_isochrones.total_bounds)  # (minx, miny, maxx, maxy)
    gdf_poi = get_fsq_points(bbox=bbox, poi_category=poi_category)
    # gdf_poi = gdf_poi.sjoin(gdf_isochrones)
    # Perform a spatial join to align GeoSeries geometries with the GeoDataFrame
    gdf_poi = gpd.sjoin(gdf_poi, gdf_isochrones.to_frame('geometry'), how='inner', predicate='intersects')
    # print(gdf_isochrones.columns)
    return gdf_poi