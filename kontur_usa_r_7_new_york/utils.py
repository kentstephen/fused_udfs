@fused.cache
def get_kontur():
    import geopandas as gpd
    import requests
    import pandas as pd
    import gzip
    from io import BytesIO
    
    # URL for the gzipped GeoPackage file
    url = "https://geodata-eu-central-1-kontur-public.s3.amazonaws.com/kontur_datasets/kontur_population_US_20231101.gpkg.gz"
    
    # Fetch the gzipped GeoPackage file
    response = requests.get(url)
    response.raise_for_status()
    
    # Decompress the gzipped content
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
        decompressed_file = BytesIO(gz.read())
    
    # Load the GeoPackage into a GeoDataFrame
    gdf = gpd.read_file(decompressed_file)
    gdf = gdf[['h3', 'population']]
    df = pd.DataFrame(gdf)
    return df
@fused.cache
def get_cells(df_tract, df_kontur):
    # xmin, ymin, xmax, ymax = bounds
    con = fused.utils.common.duckdb_connect()
    print(df_tract)



    
    
    query = """
    WITH filtered_hex AS (
    SELECT h3_string_to_h3(h3) as hex, population
    FROM df_kontur
  WHERE h3_cell_to_lat(hex) >= 40.48
        AND h3_cell_to_lat(hex) < 45.02
        AND h3_cell_to_lng(hex) >= -79.76
        AND h3_cell_to_lng(hex) < -71.79
)
SELECT h3_cell_to_parent(f.hex, 7) as hex,
       SUM(f.population) as pop
FROM filtered_hex f
JOIN df_tract t 
    ON ST_Intersects(
        ST_GeomFromText(h3_cell_to_boundary_wkt(f.hex)), 
        ST_GeomFromText(t.geometry)
    )
GROUP BY hex;
   
    """
    # print(query)

    df = con.sql(query).df()
    return df
@fused.cache
def add_rgb(df, value_column, n_quantiles=10):
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
    cmap = plt.cm.plasma
    
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
    