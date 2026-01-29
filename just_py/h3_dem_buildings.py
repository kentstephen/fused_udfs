@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    gdf_overture = fused.run(
        "UDF_Overture_Maps_Example",
        theme="buildings",
        overture_type="building",
        bbox=bbox,
        min_zoom=0
    )
    x, y, z = bbox.iloc[0][["x", "y", "z"]]
    df_dem = fused.run("UDF_DEM_Tile_Hexify", x=x, y=y, z=z)
    def run_query(df_dem):
        con = fused.utils.common.duckdb_connect()
        query="""SELECT
         h3_cell_to_boundary_wkt(hex) as boundary,
         metric
         from df_dem
        """
        df = con.sql(query).df()
        gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
        return gdf

    gdf_dem = run_query(df_dem)
    gdf_joined = gdf_overture.sjoin(gdf_dem)
    return gdf_joined