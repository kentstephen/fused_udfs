@fused.cache
def add_rgb(df, value_column, n_quantiles=15):
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
    
    # Drop NA values for quantile calculation
    valid_data = df[value_column].dropna()
    
    # Calculate quantiles for non-null values
    quantiles = pd.qcut(valid_data, q=n_quantiles, labels=False, duplicates='drop')
    
    # Normalize using the quantiles themselves, as in original
    norm = mcolors.Normalize(vmin=quantiles.min(), vmax=quantiles.max())
    cmap = plt.cm.cividis
    
    # Function to convert normalized quantile values to RGB
    def map_to_rgb(value):
        if pd.isna(value):
            return 0, 0, 0
        color = cmap(norm(value))
        return (int(color[0] * 255), int(color[1] * 255), int(color[2] * 255))
    
    # Create a Series of quantiles aligned with original DataFrame
    full_quantiles = pd.Series(index=df.index)
    full_quantiles.loc[valid_data.index] = quantiles
    
    # Apply function and add RGB columns
    rgb_values = full_quantiles.apply(map_to_rgb)
    df[['r', 'g', 'b']] = pd.DataFrame(rgb_values.tolist(), index=df.index)
    
    return df
@fused.cache
def get_texas_boundary_wkt():
    con = fused.utils.common.duckdb_connect()
    wkt_geom = con.sql("""
select
    ST_AsText(geometry) AS wkt
from read_parquet('s3://overturemaps-us-west-2/release/2024-06-13-beta.0/theme=divisions/type=division_area/*', filename=true, hive_partitioning=1)
where
    country = 'US'
    and region = 'US-TX'
    and subtype = 'region'
"""
    ).fetchall()
    return wkt_geom