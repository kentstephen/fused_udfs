@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    import duckdb
    from utils import add_rgb
    @fused.cache
    def read_data():
        return duckdb.sql("from read_parquet('s3://fused-users/stephenkentdata/hospitals_new_england_r_10_iso.parquet')").df()
    df_iso = read_data()
    df_kontur = fused.run("fsh_1Kg290kb0BPNahbrby4Kah", bbox=bbox)
    def run_query(df_iso, df_kontur):
        con = fused.utils.common.duckdb_connect()
        query="""
        select
            i.hex,
            i.cnt as cnt,
            coalesce(k.pop, 0) as pop
        from df_iso i
        left join df_kontur k on i.hex = k.hex
            
        """
        return con.sql(query).df()
    df = run_query(df_iso, df_kontur)
    # print(df)
    df = add_rgb(df, 'cnt')
    return df

  