@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int=9,
        grid_distance: int = 1
):
    import geopandas as gpd
    import shapely
    # grid
    gdf_road = fused.run("fsh_334qum5BO970rLjaMFAykg", bbox=bbox, resolution=resolution, grid_distance=grid_distance)
    gdf_road["total_value"] = gdf_road["total_value"] * 100
    print(f"gdf_road tv:{gdf_road['total_value'].describe()}")
    
    gdf_overture = fused.run(
        "UDF_Overture_Maps_Example",
        theme="buildings",
        overture_type="building",
        bbox=bbox
    )
    gdf_joined = gdf_overture.sjoin(gdf_road, how='left', predicate='intersects')
    print(gdf_joined)
    return gdf_joined
    