@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds=None,
        res: int =14,
        stats_type: str = "mean"
):
    import duckdb
    udf_elev ="fsh_7aUGFL8IM5tddfanL7nsfH"
    udf_flow = "fsh_4kxCe8ZbfIFhKzj2rmpNW2"
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    df_elev, df_flow = common.run_pool(
        lambda token: fused.run(token, res=res, bounds=bounds),
        [udf_elev, udf_flow],
        max_workers=2
    ) 
    if df_elev is None or df_flow is None or df_elev.empty or df_flow.empty:
        return
    qr = """select 
               e.hex,
             --  e.metric as elev,
            --  f.metric as flow,
            (e.metric - 1900) - (1 + LOG10(least(greatest(f.metric)) + 1) * 0.9) as e_f,
           -- (e.metric - 1900) + log(least(greatest(f.metric, 0), 100) + 1) as e_f
              -- e.metric + (1 + log10(f.metric) * 0.1) as e_f,
            --  (e.metric - 1900) + log(least(greatest(f.metric, 0), 100) + 1) as e_f
              --e.metric * (1 + f.metric * 0.01) as e_f
             -- e.metric * (1 + log10(f.metric) * 0.1) as e_f,
       --     (e.metric - 1900) * (1 + LOG10(GREATEST(f.metric + 1, 1)) * 0.0094) as e_f
            --   (e.metric - 1900) * 60 + SQRT(f.metric) * 40 as e_f -- kinda weird, large values
              --(e.metric - 1900) * (1 + LOG10(least(greatest(f.metric)) + 1) * 0.05) as e_f,
             --   (e.metric - 1900) * (1 + LOG10(f.metric + 1) * 0.05) as e_f,
             -- (e.metric - 1900) * 100 + LOG10(f.metric + 1) * 500 as e_f
               --e.metric * 0.1 + LOG10(f.metric + 1) * 500 as e_f
            -- e.metric + (LOG10(f.metric + 1) * 150) as e_f -- winner for denali
           -- e.metric + (1 * f.metric * 0.1) as e_f
           --   (e.metric) + (log(f.metric) * 4) as e_f
               --(e.metric) + (f.metric * 0.01) as e_f         -- log(f.metric) + (e.metric * 0.01) as e_f
             --  2.0 * ln(f.metric) + (e.metric * 0.1) as e_f

               from df_elev e inner join df_flow f
               on e.hex=f.hex
        """

    df = duckdb.sql(qr).df()
    print(df.describe())
    print(df)
    return df