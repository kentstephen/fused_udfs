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
    "$where": "created_date >= '2020-03-01T00:00:00' AND created_date <= '2020-07-30T00:00:00' AND lower(descriptor) LIKE '%noise%'",
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

def get_emergency_cells():
    import pandas as pd
    df = get_data()
    print(df)
    con = fused.utils.common.duckdb_connect()
    query="""
    select 
    h3_latlng_to_cell(latitude::DOUBLE, longitude::DOUBLE, 13)::UBIGINT as h3_index,
   created_date
    
    from df
   -- group by 1
    """
    df = con.sql(query).pl()
    df = df.to_pandas(use_pyarrow_extension_array=True)
    # df['h3_index'] = df['h3_index'].astype('int64')
    return df
    
    
@fused.cache
def get_precincts():
    import shapely
    import geopandas as gpd
    import pandas as pd
    import fused

    bbox = gpd.GeoDataFrame(
        geometry=[shapely.box(-74.3243,40.3257,-73.5237,40.9984)], # Maryland
        crs=4326
    )
    gdf = gpd.read_file('https://data.cityofnewyork.us/api/geospatial/j2bc-fus8?method=export&format=GeoJSON')


    # Or to dissolve everything into one geometry:
    mask = gdf.dissolve()
# Ensure mask is in EPSG:4326
    if mask.crs != "EPSG:4326":
        mask = mask.to_crs("EPSG:4326")

    # Convert the lines to a polygon
    

    # Run fused function
    df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    # Filter for "Hiking Trail"
    df = df[df['fsq_category_ids'] == "4bf58dd8d48988d12e941735"] # police stataion

    # Debugging outputs
   

    # Ensure df is in EPSG:4326
    if df.crs != "EPSG:4326":
        df = df.to_crs("EPSG:4326")


    

    # Clip the target GeoDataFrame using the mask
    df = gpd.overlay(df, mask, how='intersection')

    df['geometry'] = df['geometry'].apply(shapely.wkt.dumps)
    return pd.DataFrame(df)
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

   
# station_loads AS (
#     SELECT 
#         pc.station_hex,
#         cc.calls_hex,
#         cc.call_cnt,
#         cc.month,
#         h3_cell_to_local_ij(pc.station_hex, cc.calls_hex) as ij_coords,
#         abs(h3_cell_to_local_ij(pc.station_hex, cc.calls_hex)[1]) + 
#         abs(h3_cell_to_local_ij(pc.station_hex, cc.calls_hex)[2]) as ij_distance
#     FROM precinct_cells pc
#     JOIN calls_cells cc ON pc.parent_hex = cc.parent_hex
#     WHERE h3_grid_distance(pc.station_hex, cc.calls_hex) <= 15
# )

# SELECT 
#     h3_h3_to_string(station_hex) as hex,
#     month as datetime,
#     sum(
#         CASE 
#             WHEN ij_distance = 0 THEN call_cnt
#             ELSE call_cnt::DOUBLE / ij_distance
#         END
#     ) as load_score
# FROM station_loads
# GROUP BY hex, month
