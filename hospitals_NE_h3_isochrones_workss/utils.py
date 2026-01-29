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

def get_isochrone_cells(bounds, df_iso_stg):
    xmin, ymin, xmax, ymax = bounds
    con = fused.utils.common.duckdb_connect()
    query = """
    SELECT *
    FROM df_iso_stg
    WHERE h3_cell_to_lat(hex) >= $ymin
    AND h3_cell_to_lat(hex) < $ymax
    AND h3_cell_to_lng(hex) >= $xmin
    AND h3_cell_to_lng(hex) < $xmax
    
    """
    return con.sql(query, params={'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax}).df()
@fused.cache
def get_mask_wkt():
    import shapely
    import geopandas as gpd
    import pandas as pd
    nh_mask = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/33_NEW_HAMPSHIRE/33/tl_2020_33_tract20.zip')
    me_mask = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/23_MAINE/23/tl_2020_23_tract20.zip')
    vt_mask = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/50_VERMONT/50/tl_2020_50_tract20.zip')
    
    # Combine all and dissolve into a single geometry
    combined_mask = gpd.GeoDataFrame(pd.concat([nh_mask, me_mask, vt_mask])).dissolve().geometry.iloc[0]
    return shapely.wkt.dumps(combined_mask)