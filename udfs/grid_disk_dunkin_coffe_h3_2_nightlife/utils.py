import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pandas as pd
import numpy as np

def add_rgb(df, value_column, n_quantiles=10, min_percentile=1, max_percentile=99):
    # Check if the column is empty or all values are null
    if df[value_column].isnull().all() or len(df) == 0:
        print(f"Warning: Column '{value_column}' is empty or contains only null values.")
        df['r'] = df['g'] = df['b'] = 0  # Assign black color to all rows
        return df

    # Remove null values for calculations
    valid_data = df[value_column].dropna()

    if len(valid_data) == 0:
        print(f"Warning: No valid data in column '{value_column}' after removing null values.")
        df['r'] = df['g'] = df['b'] = 0  # Assign black color to all rows
        return df

    # Calculate min and max values based on percentiles to exclude extreme outliers
    min_val = np.percentile(valid_data, min_percentile)
    max_val = np.percentile(valid_data, max_percentile)
    
    # Clip values to the range [min_val, max_val]
    clipped_values = np.clip(valid_data, min_val, max_val)
    
    # Create a custom normalization
    norm = mcolors.Normalize(vmin=min_val, vmax=max_val)
    
    # Use a perceptually uniform colormap like 'viridis'
    cmap = plt.cm.cividis
    
    # Function to convert normalized values to RGB
    def map_to_rgb(value):
        if pd.isnull(value):
            return 0, 0, 0  # Return black for null values
        color = cmap(norm(value))
        r = int(color[0] * 255)
        g = int(color[1] * 255)
        b = int(color[2] * 255)
        return r, g, b
    
    # Apply function and add RGB columns to DataFrame
    df[['r', 'g', 'b']] = df[value_column].apply(map_to_rgb).apply(pd.Series)
    
    return df


def get_con():
    import duckdb
    con = duckdb.connect()
    con.sql(""" INSTALL h3 from community;
                LOAD h3;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_region='us-west-2';""")
    return con