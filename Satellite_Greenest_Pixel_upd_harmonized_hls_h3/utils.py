#fire version one ["B12", "B11", "B8A"]
# Or for even more dramatic burn scars:
# "band_list": ["B11", "B12", "B8A"],  # Swap B12 and B11

# Or for burn scar emphasis, try this band combo instead:
# "band_list": ["B12", "B8A", "B04"],  # SWIR2, NIR, Red

def df_to_hex(bounds, df, res, latlng_cols=("lat", "lng")):
    xmin, ymin, xmax, ymax = bounds
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            array_agg(data) as agg_data
            FROM df
            where
    h3_cell_to_lat(hex) >= {ymin}
    AND h3_cell_to_lat(hex) < {ymax}
    AND h3_cell_to_lng(hex) >= {xmin}
    AND h3_cell_to_lng(hex) < {xmax}

            group by 1
          -- order by 1
        """
    con = utils.duckdb_connect()
    return con.query(qr).df()
    

    # return con.query(qr).df()
def aggregate_df_hex(bounds, df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import pandas as pd
    import numpy as np
    
    # Convert to hexagons
    df = df_to_hex(bounds,df, res=res, latlng_cols=latlng_cols)
    
    # Define aggregation functions that handle null values
    if stats_type == "sum":
        fn = lambda x: pd.Series(x).fillna(0).sum()
    # elif stats_type == "mean":
    #     fn = lambda x: np.maximum(0, np.array([val for val in x if val is not None], dtype=float)).mean()
    else:
        fn = lambda x: pd.Series(x).mean()
    
    # Apply the aggregation function to create the metric column
    df["metric"] = df.agg_data.map(fn)
    del df['agg_data']
    # Replace any remaining NaN values with 0
    # df["metric"] = df["metric"].fillna(0)
    
    return df