@fused.cache
def get_gdf(): 
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point
    import numpy as np

    df = pd.read_csv('https://data.cityofnewyork.us/api/views/8586-3zfm/rows.csv?accessType=DOWNLOAD&api_foundry=true')
    df["award"] = df["Construction Award"] / 100000
    # df["award"] = df["award"] / 1000
    # Remove parentheses and split into latitude and longitude
    df[['latitude', 'longitude']] = df['Location 1'].str.strip('()').str.split(',', expand=True)

    # Convert latitude and longitude to float
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)

    # Create a geometry column from the lat/lon
    df['geometry'] = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)

    # Convert to a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    # Set a coordinate reference system (CRS) if needed, e.g., EPSG:4326 for lat/lon
    gdf.set_crs(epsg=4326, inplace=True)

    return gdf
