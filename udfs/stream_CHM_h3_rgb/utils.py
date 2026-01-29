# @fused.cache
def df_to_hex(df, res,bounds_values, latlng_cols=("lat", "lng")):
    xmin, ymin, xmax, ymax = bounds_values
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
# @fused.cache
def aggregate_df_hex(df, res,bounds_values, latlng_cols=("lat", "lng"), stats_type="mean"):
    import pandas as pd
    import numpy as np
    
    # Convert to hexagons
    df = df_to_hex(df, res=res, bounds_values=bounds_values,latlng_cols=latlng_cols)
    
    # Define aggregation functions that handle null values
    if stats_type == "sum":
        fn = lambda x: pd.Series(x).fillna(0).sum()
    # elif stats_type == "mean":
    #     fn = lambda x: np.maximum(0, np.array([val for val in x if val is not None], dtype=float)).mean()
    else:
        fn = lambda x: pd.Series(x).mean()
    
    # Apply the aggregation function to create the metric column
    df['metric'] = df.agg_data.map(fn)
    
    # Replace any remaining NaN values with 0
    # df["metric"] = df["metric"].fillna(0)
    
    return df

def add_rgb(df, value_column, n_quantiles=10):
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import pandas as pd
    import numpy as np

    # Check if the DataFrame has data
    if df.empty or df[value_column].nunique() < 2:
        # Add default RGB columns with NaNs or a default color
        df[['r', 'g', 'b']] = np.nan
        return df

    # Calculate quantiles for the value column
    quantiles = pd.qcut(df[value_column], q=n_quantiles, labels=False, duplicates='drop')
    
    # Normalize the quantile values between 0 and 1
    norm = mcolors.Normalize(vmin=quantiles.min(), vmax=quantiles.max())
    cmap = plt.cm.Greens  # Put the color map name here at the end - case sensitive
    
    # Function to convert normalized quantile values to RGB
    def map_to_rgb(q):
        color = cmap(norm(q))
        r = int(color[0] * 255)
        g = int(color[1] * 255)
        b = int(color[2] * 255)
        return r, g, b
    
    # Apply function and add RGB columns to DataFrame using quantile values
    df[['r', 'g', 'b']] = quantiles.apply(map_to_rgb).apply(pd.Series)
    return df