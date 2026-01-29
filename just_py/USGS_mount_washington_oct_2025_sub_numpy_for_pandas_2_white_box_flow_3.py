# utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds=None,
        res: int =10,
        stats_type: str = "max"
):

    import numpy as np
    import pandas as pd
    # import rioxarray
    # import fsspec
    dem_hex = fused.load("https://github.com/fusedio/udfs/tree/f5038bc/community/sina/DEM_Tile_Hexify/")
    utils = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    # zoom = utils.estimate_zoom(bounds)
    # Read and combine all TIFF files
    df_tiff = fused.run('fsh_279pGvzVhZSSzDmeSdjBTy', bounds=bounds) #flow wbt
    print(f"df_tiff:{df_tiff}")

    if df_tiff is None or df_tiff.empty:
        return None

    print(f"Combined dataframe shape: {df_tiff.shape}")

    # Hexagonify  aggregate
    df = aggregate_df_hex(
       bounds, df_tiff, res=res, latlng_cols=["y", "x"], stats_type=stats_type
    )

    if df is None or df.empty:
        return None

    print(df)
    return df


@fused.cache
def df_to_hex(bounds, df, res, latlng_cols=("lat", "lng")):  
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    bounds = common.bounds_to_gdf(bounds) 
    bounds_values = bounds.bounds.values[0]
    xmin, ymin, xmax, ymax = bounds_values
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
                   ARRAY_AGG(flow) as agg_data
            FROM df
            WHERE h3_cell_to_lat(hex) >= {ymin}
              AND h3_cell_to_lat(hex) < {ymax}
              AND h3_cell_to_lng(hex) >= {xmin}
              AND h3_cell_to_lng(hex) < {xmax}
            GROUP BY 1
        """
    con = common.duckdb_connect()
    return con.query(qr).fetchnumpy()  # Returns dict of numpy arrays

@fused.cache
def aggregate_df_hex(bounds, df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import numpy as np
    import pandas as pd
    
    result = df_to_hex(bounds, df, res=res, latlng_cols=latlng_cols)
    
    # result is {'hex': array(...), 'agg_data': array of lists}
    hex_arr = result['hex']
    agg_data_arr = result['agg_data']
    
    # Apply numpy function to each list in the array
    if stats_type == "max":
        metric = np.array([np.max(x) for x in agg_data_arr])
    else:
        metric = np.array([np.mean(x) for x in agg_data_arr])
    
    return pd.DataFrame({'hex': hex_arr, 'metric': metric})

