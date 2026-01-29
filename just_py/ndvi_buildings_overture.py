@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=10):
    import geopandas as gpd
    import shapely

    def get_ndvi(resolution):
        x, y, z = bbox.iloc[0][["x", "y", "z"]]
        df = fused.run("fsh_5ERBSj86Xq8ehqUu1FBjBs", x=x, y=y, z=z)
        con = fused.utils.common.duckdb_connect()
        qr = """
        select
        hex,
        h3_cell_to_boundary_wkt(hex) as boundary,
         data
         from df
         where data >= 0.7"""
        df = con.sql(qr).df()
        # Scale up the values by a factor of 10
        df['data'] = df['data'] * 100

        return  gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads), crs='EPSG:4326')
    gdf_ndvi = get_ndvi(resolution)
    
    print(gdf_ndvi.crs)
    # print(gdf_ndvi)
    gdf_overture = fused.run(
        "UDF_Overture_Maps_Example",
        theme="buildings",
        overture_type="building",
        bbox=bbox,
        min_zoom=0
    )
    # gdf_ndvi = gdf_ndvi.to_crs(gdf_overture.crs)
    print(gdf_overture)
    gdf_joined = gdf_overture.sjoin(gdf_ndvi)
    print(gdf_joined["data"].describe())
    return gdf_joined