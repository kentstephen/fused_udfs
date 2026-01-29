@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=10):
    import geopandas as gpd
    import shapely
    df = fused.run("fsh_5lgqCUfgRq6xEyw4BWMK8T", resolution=resolution, bbox=bbox)
    print(type(df))
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=10)
    # if len(gdf_overture) < 1:
    #     return
    gdf_joined = gdf_overture.sjoin(df, how="inner", predicate="intersects")
    return gdf_joined