import duckdb
def get_con():
    
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
                LOAD h3ext;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;""")
    return con
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pandas as pd
import numpy as np
def add_rgb(df, value_column, n_quantiles=10):
    # Check if DataFrame is empty or the value_column has no data
    if df.empty or df[value_column].isnull().all():
        # If empty, return the DataFrame without trying to apply quantiles
        return df
    
    # Proceed with quantile calculation
    quantiles = pd.qcut(df[value_column], q=n_quantiles, labels=False, duplicates='drop')
    
    # Normalize the quantile values between 0 and 1
    norm = mcolors.Normalize(vmin=quantiles.min(), vmax=quantiles.max())
    cmap = plt.cm.viridis
    
    def map_to_rgb(q):
        color = cmap(norm(q))
        r = int(color[0] * 255)
        g = int(color[1] * 255)
        b = int(color[2] * 255)
        return r, g, b
    
    # Apply the function and add RGB columns to DataFrame
    df[['r', 'g', 'b']] = quantiles.apply(map_to_rgb).apply(pd.Series)
    return df
