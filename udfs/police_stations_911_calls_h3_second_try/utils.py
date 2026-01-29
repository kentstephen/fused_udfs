@fused.cache
def get_emergency_calls():
    import requests
    import pandas as pd
    import io
    
    def fetch_data(offset):
       """Fetch data chunk starting at given offset"""
       path = "https://data.cityofnewyork.us/resource/n2zq-pubd.csv"
       params = {
           "$limit": 50000,
           "$offset": offset
       }
       return requests.get(path, params=params)
    
    # Create list of offsets up to 6 million
    offsets = range(0, 6_000_000, 50000)
    
    # Use run_pool with full path to fetch all chunks in parallel
    responses = fused.utils.common.run_pool(fetch_data, offsets)
    
    # Process all successful responses
    dfs = []
    for response in responses:
       if response.status_code == 200:
           chunk_df = pd.read_csv(io.StringIO(response.text))
           dfs.append(chunk_df)
    
    # Combine all chunks and select columns
    if dfs:
       df = pd.concat(dfs, ignore_index=True)
       df = df[['typ_desc', 'create_date', 'latitude', 'longitude']]
    return df

def get_emergency_cells():
    df = get_emergency_calls()
    # print(df)
    con = fused.utils.common.duckdb_connect()
    query="""
    select 
    h3_latlng_to_cell(latitude, longitude, 15) as h3_index,
    create_date
    
    from df
   -- group by 1
   where typ_desc like '%DISORDERLY%'
    """
    return con.sql(query).df()
    
    
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
