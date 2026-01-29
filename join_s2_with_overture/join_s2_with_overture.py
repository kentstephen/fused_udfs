@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=12,
        time_of_interest="2024-08-15/2024-11-01"
       ):
    from utils import add_rgb
    import geopandas as gpd
    import shapely
    import pandas as pd
    # h3_buildings
    x, y, z = bbox.iloc[0][["x", "y", "z"]]
    print(z)
    
    df_buildings = fused.run("fsh_2VS2A0sNhUNtcW8AzUeDrq", resolution=resolution, x=x, y=y, z=z)
    if df_buildings is None or df_buildings.empty:
        return pd.DataFrame()
    df_sentinel = fused.run("fsh_5pY9xVLSE3XP0IlpeaYnlT", resolution=resolution, time_of_interest=time_of_interest, x=x, y=y, z=z)
    
    def run_query(df_buildings, df_sentinel):
        con = fused.utils.common.duckdb_connect()
        query = """
        SELECT
        b.cell_id,
        COALESCE(s.data, 0) as data
        FROM df_buildings b
        LEFT JOIN df_sentinel s ON b.cell_id = s.hex
        """
        return con.sql(query).df()

    df = run_query(df_buildings=df_buildings, df_sentinel=df_sentinel)
    df = add_rgb(df, 'data')
    print(df["data"].describe())
    print(df)
    return df