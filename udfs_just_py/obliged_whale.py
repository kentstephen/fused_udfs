@fused.udf
def udf(bounds: fused.types.Bounds = None, 
        resolution: int = 14
): # [-74.033,40.648,-73.788,40.846]
    common = fused.load("https://github.com/fusedio/udfs/tree/6dd2c4e/public/common/")
    import duckdb
    # df_overture= fused.run(', bounds=bounds,resolution=resolution)
    # df_cab = get_cab(resolution)

    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    df_overture, df_cab = common.run_pool(
        lambda token: fused.run(token, bounds=bounds,resolution=resolution),
        ["fsh_7EiRMtV3XKj72UYGqS0Psg", "fsh_1ZprMGCYRfDzKtkkybZ28d"],
        max_workers=2
    ) 
    if df_overture is None or df_cab is None or df_overture.empty or df_cab.empty:
        return
    qr = """
select c.hex,
        c.cnt
from df_cab c
inner join df_overture o on c.hex != o.hex

-- st_disjoint(st_geomfromtext(h3_cell_to_boundary_wkt(c.hex)), st_geomfromtext(o.geometry_wkt)) 
    """
    con = common.duckdb_connect()
    df = con.sql(qr).df()
    print(df)
    return df
# @fused.cache
# def get_cab(resolution):
#     return fused.run('fsh_1ZprMGCYRfDzKtkkybZ28d', resolution=resolution)