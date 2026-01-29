@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=13,
       min_count: int=1):
    from utils import add_rgb
    import geopandas as gpd
    import shapely

    df_buildings = fused.run("fsh_10rqlgCZfA0GisBxNSE1Gs", bbox=bbox, resolution=14)
    @fused.cache
    def get_cabs(resolution, min_count):
        df = fused.run("fsh_4eXAPGRXsPOdwNfZwWjYeV", resolution=11, min_count=min_count)
        return df
    df_cabs = get_cabs(resolution, min_count)
    def run_query(df_buildings, df_cabs):
        con = fused.utils.common.duckdb_connect()
        query="""
        SELECT
        b.cell_id,
        b.height,
        cnt
        FROM df_buildings b
        INNER JOIN df_cabs c ON h3_cell_to_parent(b.cell_id, 11) = c.cell_id
        """
        return con.sql(query).df()

    df = run_query(df_buildings, df_cabs)
    df = add_rgb(df, 'cnt')
    print(df["cnt"].describe())
    print(df)
    return(df)