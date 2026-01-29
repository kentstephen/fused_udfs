@fused.cache(path="s3://fused-users/stephenkentdata/fused-cache/stephen.kent.data@gmail.com/walkability_index/")
def get_data():
    import geopandas as gpd
    url = "/vsizip//vsicurl/https://edg.epa.gov/EPADataCommons/public/OA/WalkabilityIndex.zip/Natl_WI.gdb"
    gdf = gpd.read_file(url, engine="pyogrio")
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    print(gdf)
    return gdf
@fused.cache(path="s3://fused-users/stephenkentdata/fused-cache/stephen.kent.data@gmail.com/walkability_index/")
def get_walk_data():
    import pandas as pd
    import pyarrow as pa
    from shapely import wkt
    gdf = get_data()
    df = pd.DataFrame(gdf)
    df['geometry'] = df['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    df["walk_score"] = df["NatWalkInd"]
    df = df[["geometry", "walk_score"]]
    return pa.Table.from_pandas(df)

def add_rgb(df, value_column, global_quantiles=None, n_quantiles=7):
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
        
        if global_quantiles is not None:
            # Use pre-calculated quantile labels
            quantiles = pd.cut(non_zero_data, 
                             bins=global_quantiles, 
                             labels=False, 
                             include_lowest=True)
        else:
            # Calculate quantiles for this subset
            quantiles = pd.qcut(non_zero_data, 
                              q=n_quantiles, 
                              labels=False, 
                              duplicates='drop')
        
        # Create normalization and apply colormap
        norm = mcolors.Normalize(vmin=0, vmax=n_quantiles-1)
        cmap = plt.cm.plasma
        colors = cmap(norm(quantiles))
        rgb_values = (colors[:, :3] * 255).astype(int)
        
        # Update non-zero values with colormap colors
        df.loc[non_zero_mask, ['r', 'g', 'b']] = rgb_values
    
    return df