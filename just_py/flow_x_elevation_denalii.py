@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds=None,
        res: int =12,
        stats_type: str = "mean"
):
    import duckdb
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    df_elev, df_flow = common.run_pool(
        lambda token: fused.run(token, res=res, bounds=bounds),
        ["fsh_4WhZcoHMwpCjS51mb0K1UV", "fsh_5IScZJhzFoI2ZWvBuJUuqo"],
        max_workers=2
    ) 
    if df_elev is None or df_flow is None or df_elev.empty or df_flow.empty:
        return
    qr = """select 
               e.hex,
               e.metric as elev,
               f.metric as flow,
               e.metric + (LOG10(f.metric + 1) * 375) as e_f -- winner
           --    e.metric + (1 * f.metric * 0.01) as e_f
           --   (e.metric) + (log(f.metric) * 4) as e_f
               --(e.metric) + (f.metric * 0.01) as e_f         -- log(f.metric) + (e.metric * 0.01) as e_f
             --  2.0 * ln(f.metric) + (e.metric * 0.1) as e_f

               from df_elev e inner join df_flow f
               on e.hex=f.hex
        """

    df = duckdb.sql(qr).df()
    print(df)
    return df