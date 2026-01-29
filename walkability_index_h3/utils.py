@fused.cache
def get_data():
    import geopandas as gpd
    import pandas as pd
    from shapely import wkt
    url = "/vsizip//vsicurl/https://edg.epa.gov/EPADataCommons/public/OA/WalkabilityIndex.zip/Natl_WI.gdb"
    gdf = gpd.read_file(url, engine="pyogrio")
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    return gdf
@fused.cache
def get_walk_polygons():
    import pandas as pd
    import pyarrow as pa
    # Get the original GDF
    gdf = get_data()
    
    # Create a new GDF with just the columns we need
    simplified_gdf = gdf[["geometry", "NatWalkInd"]].copy()
    simplified_gdf = simplified_gdf.rename(columns={"NatWalkInd": "walk_score"})
    
    # Explode the MultiPolygons
    exploded_gdf = simplified_gdf.explode(index_parts=True)
    
    # Reset the index
    exploded_gdf = exploded_gdf.reset_index(drop=True)
    
    # Convert to DataFrame and convert geometry to WKT
    df = pd.DataFrame(exploded_gdf)
    df['geometry'] = df['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    df = df[["geometry", "walk_score"]]
    return pa.Table.from_pandas(df)


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