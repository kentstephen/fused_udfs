# @fused.cache
def add_rgb(df, value_column, n_quantiles=20):
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import pandas as pd
    import numpy as np
    
    # Handle empty dataframe or all null values
    if df.empty or df[value_column].isna().all():
        df['r'] = 0
        df['g'] = 0
        df['b'] = 0
        return df
    
    # Initialize RGB columns
    df[['r', 'g', 'b']] = 0
    
    # Set grey color for zeros
    zero_mask = df[value_column] == 0
    df.loc[zero_mask, ['r', 'g', 'b']] = 184
    
    # Handle non-zero values with colormap
    non_zero_mask = (df[value_column] != 0) & (~df[value_column].isna())
    if non_zero_mask.any():
        # Get non-zero values
        non_zero_data = df.loc[non_zero_mask, value_column]
        
        # Calculate quantiles for non-zero values
        quantiles = pd.qcut(non_zero_data, q=n_quantiles, labels=False, duplicates='drop')
        
        # Create normalization and apply colormap
        norm = mcolors.Normalize(vmin=quantiles.min(), vmax=quantiles.max())
        cmap = plt.cm.plasma
        colors = cmap(norm(quantiles))
        rgb_values = (colors[:, :3] * 255).astype(int)
        
        # Update non-zero values with colormap colors
        df.loc[non_zero_mask, ['r', 'g', 'b']] = rgb_values
    
    return df