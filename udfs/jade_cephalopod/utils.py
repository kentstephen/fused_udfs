def get_points(n):
    import random
    import pandas as pd
    import time
    
    # Define the bounding box (min_lat, max_lat, min_lon, max_lon)
    min_lat, max_lat = 40.4774, 40.9176  # New York City bounding box
    min_lon, max_lon = -74.2591, -73.7002
    
    # Function to generate random latitude and longitude
    def generate_random_coordinates(min_lat, max_lat, min_lon, max_lon, n):
        latitudes = [random.uniform(min_lat, max_lat) for _ in range(n)]
        longitudes = [random.uniform(min_lon, max_lon) for _ in range(n)]
        return latitudes, longitudes
    
    # Generate 10 random coordinates and store them in a DataFrame
  
    latitudes, longitudes = generate_random_coordinates(min_lat, max_lat, min_lon, max_lon, n)
    df = pd.DataFrame({'Latitude': latitudes, 'Longitude': longitudes})
    time.sleep(10)
    return df
import duckdb
def get_con():
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3 FROM community;
                LOAD h3;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;""")
    return con

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pandas as pd
import numpy as np

def add_rgb_to_df(df, value_column, n_quantiles=10):
    # Calculate quantiles for the value column
    quantiles = pd.qcut(df[value_column], q=n_quantiles, labels=False, duplicates='drop')
    
    # Normalize the quantile values between 0 and 1
    norm = mcolors.Normalize(vmin=quantiles.min(), vmax=quantiles.max())
    cmap = plt.cm.plasma  # Still using the 'plasma' colormap for consistency
    
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
