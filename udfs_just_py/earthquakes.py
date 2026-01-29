@fused.udf
def udf(bbox: fused.types.TileGDF = None):
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point
    from shapely import box
    from utils import get_df
    bounds = bbox.bounds.values[0] if bbox is not None else default_bbox.bounds
    xmin, ymin, xmax, ymax = bounds
    # Load the earthquake data
    df = get_df()
    # Convert the DataFrame to a GeoDataFrame
    df['geometry'] = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    df["mag_value"] = df["mag"] * 100
    print(df["mag_value"].describe())
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    # Set the initial CRS to WGS84 (lat/lon)
    gdf.set_crs(epsg=4326, inplace=True)
    
    # Transform the CRS to Web Mercator (EPSG:3857) for meter-based calculations
    gdf = gdf.to_crs(epsg=3857)

    # Function to calculate buffer distance in meters based on mag and depth
    def calculate_buffer(mag, depth):
    # Avoid negative buffer sizes by taking the absolute value of the magnitude
        mag = abs(mag)  
        
        # Avoid division by zero or near-zero depths
        if depth <= 0:
            depth = 0.1  # Set a small fallback value
            
        return mag * 60000 / (depth + 10)


    # Apply the buffer in meters based on magnitude and depth
    gdf['geometry'] = gdf.apply(lambda row: row['geometry'].buffer(calculate_buffer(row['mag'], row['depth'])), axis=1)
    

    gdf = gdf.to_crs('EPSG:4326')
    bounding_box = box(xmin, ymin, xmax, ymax)

    # Load only the data within the bounding box
    gdf = gdf[gdf.intersects(bounding_box)]
    # Return the GeoDataFrame
    return gdf
