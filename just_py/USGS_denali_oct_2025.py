# utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds=None,
        res: int =11,
        stats_type: str = "mean"
):

    import numpy as np
    import pandas as pd
    # import rioxarray
    # import fsspec
    dem_hex = fused.load("https://github.com/fusedio/udfs/tree/f5038bc/community/sina/DEM_Tile_Hexify/")
    utils = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    zoom = utils.estimate_zoom(bounds)
    # Read and combine all TIFF files
    df_tiff = fused.run('fsh_5HFSGuSICBxcJjZ6qbLIFQ', bounds=bounds)
    print(f"df_tiff:{df_tiff}")

    if df_tiff is None or df_tiff.empty:
        return None

    print(f"Combined dataframe shape: {df_tiff.shape}")

    # Hexagonify & aggregate
    df = aggregate_df_hex(
        df_tiff, res, latlng_cols=["y", "x"], stats_type=stats_type
    )

    if df is None or df.empty:
        return None
    # Clean up columns if needed
    # if 'agg_data' in df.columns:
    #     df = df.drop('agg_data', axis=1)
    del df['agg_data']
    # df["elev_scale"] = int((15 - zoom) * 1)
    # Apply elevation offset if needed
    # df['metric'] = df['metric'] - 500
    # df = df[df['metric']<=813]]
    print(df)
    return df

@fused.cache
def df_to_hex(df, res, latlng_cols=("lat", "lng")):  
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, ARRAY_AGG(data) as agg_data
            FROM df
            group by 1
          --  order by 1
        """
    con = common.duckdb_connect()
    return con.query(qr).df()


@fused.cache
def aggregate_df_hex(df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import pandas as pd

    df = df_to_hex(df, res=res, latlng_cols=latlng_cols)
    if stats_type == "sum":
        fn = lambda x: pd.Series(x).sum()
    elif stats_type == "mean":
        fn = lambda x: pd.Series(x).mean()
    else:
        fn = lambda x: pd.Series(x).mean()
    df["metric"] = df.agg_data.map(fn)
    return df
