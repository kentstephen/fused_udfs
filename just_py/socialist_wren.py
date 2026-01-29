@fused.udf
def udf(name: str = "world"):
    import pandas as pd

    return pd.DataFrame({"hello": [name]})




def df_to_hex_wbt(bounds, df, res, latlng_cols=("x", "y"), dtype='wbt'):  
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    bounds = common.bounds_to_gdf(bounds) 
    bounds_values = bounds.bounds.values[0]
    xmin, ymin, xmax, ymax = bounds_values
    # if dtype == 'wbt':
    agg_type="max"
    qr = f"""
            select h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
                   max(flow) as metric
            FROM df
            where h3_cell_to_lat(hex) >= {ymin}
              and h3_cell_to_lat(hex) < {ymax}
              and h3_cell_to_lng(hex) >= {xmin}
              and h3_cell_to_lng(hex) < {xmax}
            group by 1
        """
    con = common.duckdb_connect()
    return con.query(qr).df()

@fused.cache
def df_to_hex_elevation(df, res, latlng_cols=("lat", "lng")):  
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
                   ARRAY_AGG(data) as agg_data
            FROM df
            GROUP BY 1
        """
    con = common.duckdb_connect()
    return con.query(qr).fetchnumpy()  # Returns dict of numpy arrays

@fused.cache
def aggregate_df_hex_elevation(df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import numpy as np
    import pandas as pd
    
    result = df_to_hex(df, res=res, latlng_cols=latlng_cols)
    
    # result is {'hex': array(...), 'agg_data': array of lists}
    hex_arr = result['hex']
    agg_data_arr = result['agg_data']
    
    # Apply numpy function to each list in the array
    if stats_type == "sum":
        metric = np.array([np.sum(x) for x in agg_data_arr])
    else:
        metric = np.array([np.mean(x) for x in agg_data_arr])
    
    return pd.DataFrame({'hex': hex_arr, 'metric': metric})