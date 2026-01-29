@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=10):
    import geopandas as gpd
    import shapely
    import pandas as pd

    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, use_columns=['id', 'height'], min_zoom=0)
    if gdf_overture is None or gdf_overture.empty:
        return pd.DataFrame({})
    gdf_overture = gdf_overture[['id', 'height']]
    # print(gdf_overture)
    df_overture = pd.DataFrame(gdf_overture)
    if df_overture.empty or df_overture is None:
        return pd.DataFrame({})
    def get_cells(df):
        con = fused.utils.common.duckdb_connect()
        query = """
        SELECT h3_cell_to_parent(h3_string_to_h3(SUBSTRING(id, 1, 16)), 4) as hex,
        coalesce(avg(height), 1) as height
        from df
        group by 1"""
        return con.sql(query).df()

    df = get_cells(df=df_overture)
    # df['height'] = df['height'] / 1000
    print(df)
    return df
    