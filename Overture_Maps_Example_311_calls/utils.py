run_query = fused.load(
    "https://github.com/fusedio/udfs/tree/43656f6/public/common/"
).utils.run_query

def get_overture(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-08-20-0",
    theme: str = None,
    overture_type: str = None,
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None
):
    """Returns Overture data as a GeoDataFrame."""
    import logging
    import concurrent.futures
    import json
    
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import shape, box

    # Load Fused helper functions
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f8f0c0f/public/common/"
    ).utils

    if release == "2024-02-15-alpha-0":
        if overture_type == "administrative_boundary":
            overture_type = "administrativeBoundary"
        elif overture_type == "land_use":
            overture_type = "landUse"
        theme_per_type = {
            "building": "buildings",
            "administrativeBoundary": "admins",
            "place": "places",
            "landUse": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }
    elif release == "2024-03-12-alpha-0":
        theme_per_type = {
            "building": "buildings",
            "administrative_boundary": "admins",
            "place": "places",
            "land_use": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }
    else:
        theme_per_type = {
            "address": "addresses",
            "building": "buildings",
            "infrastructure": "base",
            "land": "base",
            "land_use": "base",
            "water": "base",
            "place": "places",
            "division": "divisions",
            "division_area": "divisions",
            "division_boundary": "divisions",
            "segment": "transportation",
            "connector": "transportation",
        }

    if theme is None:
        theme = theme_per_type.get(overture_type, "buildings")

    if overture_type is None:
        type_per_theme = {v: k for k, v in theme_per_type.items()}
        overture_type = type_per_theme[theme]

    if num_parts is None:
        num_parts = 1 if overture_type != "building" else 5

    if min_zoom is None:
        if theme == "admins" or theme == "divisions":
            min_zoom = 7
        elif theme == "base":
            min_zoom = 9
        else:
            min_zoom = 12

    table_path = f"s3://us-west-2.opendata.source.coop/fused/overture/{release}/theme={theme}/type={overture_type}"
    table_path = table_path.rstrip("/")

    if polygon is not None:
        polygon=gpd.from_features(json.loads(polygon))
        bounds = polygon.geometry.bounds
        bbox = gpd.GeoDataFrame(
            {
                "geometry": [
                    box(
                        bounds.minx.loc[0],
                        bounds.miny.loc[0],
                        bounds.maxx.loc[0],
                        bounds.maxy.loc[0],
                    )
                ]
            }
        )

    def get_part(part):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            return utils.table_to_tile(
                bbox, table=part_path, use_columns=use_columns, min_zoom=min_zoom
            )
        except ValueError:
            return None

    if num_parts > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parts) as pool:
            dfs = list(pool.map(get_part, range(num_parts)))
    else:
        # Don't bother creating a thread pool to do one thing
        dfs = [get_part(0)]

    dfs = [df for df in dfs if df is not None]

    if len(dfs):
        gdf = pd.concat(dfs)

    else:
        logging.warn("Failed to get any data")
        return None

    if point_convert is not None:
        gdf["geometry"] = gdf.geometry.centroid

    return gdf




    
@fused.cache
def get_data():
    import requests
    import pandas as pd
    import io
    
    # Define API endpoint and base parameters
    path = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
    RECORDS_PER_REQUEST = 6_000_000
    
    # Define the query string for the year 2017
    query_params = {
        "$where": "created_date >= '2012-10-01T00:00:00' AND created_date <= '2012-11-30T00:00:00' ",
        "$limit": RECORDS_PER_REQUEST
    }
    
    # Send the request
    response = requests.get(path, params=query_params)
    print(response.url)  # Check the constructed URL
    if response.status_code == 200:
        # Read the data into a DataFrame
        df = pd.read_csv(io.StringIO(response.text))
        
        # Keep only the relevant columns
        df = df[['complaint_type', 'descriptor', 'created_date', 'latitude', 'longitude']]
        
        # Save to parquet format
        # df.to_parquet('311_2017.parquet')
        
        # Display the first few rows to verify
        # print(df.head())
    else:
        print(f"Error loading data: {response.status_code}, {response.text}")
    return df
def add_rgb(df, value_column, n_quantiles=10, min_percentile=1, max_percentile=99):
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import pandas as pd
    import numpy as np

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
    cmap = plt.cm.magma
    
    # Function to convert normalized values to RGB
    def map_to_rgb(value):
        if pd.isnull(value):
            return 0, 0, 0  # Return black for null values
        # Map values greater than max_val to the upper color limit
        color = cmap(norm(min(value, max_val)))
        r = int(color[0] * 255)
        g = int(color[1] * 255)
        b = int(color[2] * 255)
        return r, g, b
    
    # Apply function and add RGB columns to DataFrame
    df[['r', 'g', 'b']] = df[value_column].apply(map_to_rgb).apply(pd.Series)
    
    return df
