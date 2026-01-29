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
        wtk_geom = get_texas_boundary_wkt()
        con = fused.utils.common.duckdb_connect()
        query=f"""
        select
            i.hex,
            i.cnt as cnt,
            coalesce(k.pop, 0) as pop
        from df_iso i
        left join df_kontur k on i.hex = k.hex
        where ST_Intersects(ST_GeomFromtext(h3_cell_to_boundary_wkt(i.hex)), St_geomFromtext('{wkt_geom}'))
        """
        return con.sql(query).df()
    df = run_query(df_iso, df_kontur)
    # print(df)
    df = add_rgb(df, 'cnt')
    return df

  