def df_to_hex(df, res, bounds_values, latlng_cols=("lat", "lng")):
    xmin, ymin, xmax, ymax = bounds_values
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex,-- ARRAY_AGG(data+50) as agg_data
            FROM df
                where
                (
    h3_cell_to_lat(hex) >= {ymin}
    AND h3_cell_to_lat(hex) < {ymax}
    AND h3_cell_to_lng(hex) >= {xmin}
    AND h3_cell_to_lng(hex) < {xmax}) and data > 0
            group by 1

          --  order by 1
        """
    con = utils.duckdb_connect()
    return con.query(qr).df()

# @fused.cache
def aggregate_df_hex(df, res, bounds_values, latlng_cols=("lat", "lng"), stats_type="mean"):
    import pandas as pd

    df = df_to_hex(df, res=res, bounds_values=bounds_values, latlng_cols=latlng_cols)
    if stats_type == "sum":
        fn = lambda x: pd.Series(x).sum()
    elif stats_type == "mean":
        fn = lambda x: pd.Series(x).mean()
    else:
        fn = lambda x: pd.Series(x).mean()
    df["metric"] = df.agg_data.map(fn)
    return df
