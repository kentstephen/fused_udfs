@fused.udf
def udf(bbox: fused.types.TileGDF = None, 
        resolution: int=10,
        min_count: int=50):
    import geopandas as gpd
    import shapely

    # un commment the next line to keep 
    # @fused.cache
    def get_cabs(resolution, min_count):
        gdf = fused.run("UDF_DuckDB_H3_Example", resolution=resolution, min_count=min_count)
        return gdf
    gdf_cab = get_cabs(resolution, min_count)
    
  
    gdf_overture = fused.run(
        "UDF_Overture_Maps_Example",
        theme="buildings",
        overture_type="building",
        bbox=bbox,
        #min_zoom=0
    )
    gdf_joined = gdf_overture.sjoin(gdf_cab)
    print(gdf_joined)
    
    print(gdf_joined['cnt'].describe())
    return gdf_joined
   