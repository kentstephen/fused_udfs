def add_rgb(df, value_column, n_quantiles=5):
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import pandas as pd
    import numpy as np
    
    # Return DataFrame with empty RGB columns if empty
    if df.empty:
        df['r'] = []
        df['g'] = []
        df['b'] = []
        return df
    
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