@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely

    gdf_water = fused.run("fsh_2ZPybh8CFeuI4Hr2Vx3B2w", bbox=bbox)
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=10)

    gdf_joined = gdf_overture.sjoin(gdf_water)
    print(gdf_joined.columns)
    return gdf_joined
