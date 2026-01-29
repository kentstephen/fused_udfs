def add_rgb(df, value_column, n_quantiles=10):
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import pandas as pd
    import numpy as np
    # Calculate quantiles for the value column
    quantiles = pd.qcut(df[value_column], q=n_quantiles, labels=False, duplicates='drop')
    
    # Normalize the quantile values between 0 and 1
    norm = mcolors.Normalize(vmin=quantiles.min(), vmax=quantiles.max())
    cmap = plt.cm.plasma # Still using the 'plasma' colormap for consistency
    
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
def df_to_hex(df, res, latlng_cols=("lat", "lng")):
        utils = fused.load(
            "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
        ).utils
        qr = f"""
                SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, ARRAY_AGG(data) as agg_data
                FROM df
                group by 1
              --  order by 1
            """
        con = utils.duckdb_connect()
        return con.query(qr).df()
