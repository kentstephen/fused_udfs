@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=11,
        time_of_interest="2024-09-01/2024-11-01"
       ):
    from utils import add_rgb
    import geopandas as gpd
    import shapely
    import pandas as pd
    # h3_buildings
    x, y, z = bbox.iloc[0][["x", "y", "z"]]
    
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=10)
    if len(gdf_overture) < 1:
        return
    
    df_sentinel = fused.run("fsh_5pY9xVLSE3XP0IlpeaYnlT", resolution=resolution, time_of_interest=time_of_interest, x=x, y=y, z=z)
    
    def run_query(df_sentinel):
        con = fused.utils.common.duckdb_connect()
        query = """
        SELECT
        hex,
        h3_cell_to_boundary_wkt(hex) boundary,
        data
        FROM df_sentinel
        """
        df =con.sql(query).df()
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))

    gdf_sentinel = run_query( df_sentinel=df_sentinel)
    gdf_joined = gdf_overture.sjoin(gdf_sentinel)
    gdf_joined = gdf_joined.drop(columns=['index_right'])
    print(gdf_joined["data"].describe())
    return gdf_joined