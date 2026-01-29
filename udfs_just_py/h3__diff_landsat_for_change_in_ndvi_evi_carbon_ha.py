@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds= None,
        first_years:list = [1990, 1991],  # Multiple years
        second_years:list = [2023, 2024],
       res:int=5,
        cloud_threshold=30,
        stats_type:str="mean",
        evi:bool=True, #False returns NDVI
    ):
    import pandas as pd
    import duckdb
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)
    first_time_periods = [f"{year}-04-15/{year}-11-10" for year in first_years]
    second_time_periods = [f"{year}-04-15/{year}-11-10" for year in second_years]
    df_latlng_first, df_latlng_second = get_df_latlng(bounds, evi, cloud_threshold, first_time_periods, second_time_periods)
    if df_latlng_first is None or df_latlng_second is None or df_latlng_first.size == 0 or df_latlng_second.size == 0:
        return


    df_h3_first, df_h3_second = common.run_pool(
        lambda df: aggregate_df_hex(tile, df, res, latlng_cols=("y","x"), stats_type=stats_type),
        [df_latlng_first, df_latlng_second],
        max_workers=2
    )
    # return df_h3_first, df_h3_second
    if df_h3_first is None or df_h3_second is None or df_h3_first.empty or df_h3_second.empty:
        return

    df = get_diff(df_h3_first, df_h3_second)
   

    return df



@fused.cache
def get_diff(df_h3_first, df_h3_second):
    import numpy as np
    # df_h3_first['metric'] = df_h3_first['metric'].replace([np.inf, -np.inf], np.nan)
    # df_h3_second['metric'] = df_h3_second['metric'].replace([np.inf, -np.inf], np.nan)
    
    # # Option A: Drop rows with NaN
    # df_h3_first = df_h3_first.dropna(subset=['metric'])
    # df_h3_second = df_h3_second.dropna(subset=['metric'])
    import duckdb
    query="""
    select 
    
        df_h3_first.hex, 
        round((df_h3_second.metric - df_h3_first.metric) * 100, 3) as pct_change_evi,
        round(df_h3_first.metric, 3) as early_90s_avg,
        round(df_h3_second.metric, 3) as current_avg,
        
        -- Carbon stock estimates (tons/ha)
        round(151.7 * df_h3_first.metric - 39.76, 2) as carbon_90s_tons_ha,
        round(151.7 * df_h3_second.metric - 39.76, 2) as carbon_current_tons_ha,
        
        -- Carbon change (the constants cancel out, so simplified)
        round(151.7 * (df_h3_second.metric - df_h3_first.metric), 2) as carbon_change_tons_ha
        
    from df_h3_first 
    inner join df_h3_second 
    on df_h3_first.hex = df_h3_second.hex
    """
    return duckdb.sql(query).df()

@fused.cache#(reset=True)
def get_df_latlng(bounds,evi, cloud_threshold, first_time_periods, second_time_periods):
    import pandas as pd
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    landsat_vector = "fsh_mXQg83Ev4RZgmOoD5B2ED"
    
    # Create tuples of (period_label, time_period)
    tasks = [('first', tp) for tp in first_time_periods] + [('second', tp) for tp in second_time_periods]
    
    # Query all in parallel
    results = common.run_pool(
        lambda task: (task[0], fused.run(landsat_vector, evi=evi, bounds=bounds,cloud_threshold=cloud_threshold, time_of_interest=task[1])),
        tasks,
        max_workers=len(tasks)
    )
    
    # Separate by label
    first_dfs = [df for label, df in results if label == 'first' and df is not None and not df.empty]
    second_dfs = [df for label, df in results if label == 'second' and df is not None and not df.empty]
    
    # Concat each group
    df_latlng_first = pd.concat(first_dfs, ignore_index=True) if first_dfs else None
    df_latlng_second = pd.concat(second_dfs, ignore_index=True) if second_dfs else None
    
    return df_latlng_first, df_latlng_second
    
@fused.cache
def df_to_hex(tile, df, res, latlng_cols=("lat", "lng")): 
    #load common functions from GitHub
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    xmin, ymin, xmax, ymax = tile.geometry.iloc[0].bounds
    # create hexagons and collect the values in each cell as agg_data
    qr = f"""
        SELECT 
            h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            ARRAY_AGG(data) as agg_data
        FROM df
        WHERE
            h3_cell_to_lat(hex) >= {ymin} -- make sure we don't have overlap bewtween tiles
            AND h3_cell_to_lat(hex) < {ymax}
            AND h3_cell_to_lng(hex) >= {xmin}
            AND h3_cell_to_lng(hex) < {xmax}
        GROUP BY 1
        """
    con = common.duckdb_connect()
    return con.sql(qr).fetchnumpy() # return as a numpy array

@fused.cache
def aggregate_df_hex(tile, df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import numpy as np
    import pandas as pd
    # the result will be a numpy array which Fused can serialize
    # aggregation uses numpy as well
    result = df_to_hex(tile, df, res=res, latlng_cols=("y", "x"))

    # result is {'hex': array(...), 'agg_data': array of lists}
    hex_arr = result['hex']
    agg_data_arr = result['agg_data']

    # Apply numpy function to each list in the array
    if stats_type == "sum":
        metric = np.array([np.sum(x) for x in agg_data_arr])
    elif stats_type == "max":
        metric = np.array([np.max(x) for x in agg_data_arr]) 
    elif stats_type == "mean":
        metric = np.array([np.mean(x) for x in agg_data_arr])
    else:
        metric = np.array([np.mean(x) for x in agg_data_arr])

    return pd.DataFrame({
                        'hex': hex_arr,
                         # 'agg_data': agg_data_arr, # keep if you need: list of every value per cell
                         'metric': metric
                                        })
# @fused.cache
# def df_to_hex(da, res, bounds, stats_type):
#     import rioxarray as rio
#     common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
#     tile = common.get_tiles(bounds, clip=True)
#     # da_tiff = da.rio.write_crs("EPSG:3857").rio.reproject("EPSG:4326")
#     # df_latlng = da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]
#     return aggregate_df_hex(tile, da, res, latlng_cols=("y","x"), stats_type=stats_type)
    
# @fused.cache
# def get_h3_df(df_latlng_first, df_latlng_second, res, bounds, stats_type):
#     common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
#     tile = common.get_tiles(bounds, clip=True)
#     df_h3_first, df_h3_second = common.run_pool(
#         lambda df: aggregate_df_hex(tile, df, res, latlng_cols=("y","x"), stats_type=stats_type)
#         [df_latlng_first, df_latlng_second],
#         max_workers=2
#     )
#     return df_h3_first, df_h3_second