def add_rgb(df, value_column, n_quantiles=10):
    import matplotlib.pyplot as plt 
    import matplotlib.colors as mcolors
    import pandas as pd
    import numpy as np
    
    # Calculate quantiles excluding zeros
    non_zero_mask = df[value_column] != 0
    non_zero_values = df[value_column][non_zero_mask]
    quantiles = pd.qcut(non_zero_values, q=n_quantiles, labels=False, duplicates='drop')
    
    # Normalize the quantile values between 0 and 1
    norm = mcolors.Normalize(vmin=quantiles.min(), vmax=quantiles.max())
    cmap = plt.cm.plasma  # Change this line directly to use a different colormap
    
    def map_to_rgb(value, q=None):
        if value == 0:
            return (255, 255, 255)  # White for zero values
        color = cmap(norm(q))
        return tuple(int(x * 255) for x in color[:3])
    
    # Initialize RGB columns with white for all rows
    df[['r', 'g', 'b']] = pd.DataFrame([[255, 255, 255]] * len(df))
    
    # Update non-zero values with colored mapping
    non_zero_df = pd.DataFrame(
        [map_to_rgb(val, q) for val, q in zip(non_zero_values, quantiles)],
        index=non_zero_values.index,
        columns=['r', 'g', 'b']
    )
    df.loc[non_zero_mask, ['r', 'g', 'b']] = non_zero_df
    
    return df