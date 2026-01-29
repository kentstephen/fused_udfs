
@fused.cache
def get_gdf():
    import pandas as pd
    from shapely import wkt
    import geopandas as gpd
    buffer_distance = 40
    df = pd.read_csv('https://data.cityofnewyork.us/api/views/esmy-s8q5/rows.csv?accessType=DOWNLOAD&api_foundry=true')
    df["geometry"] = df["the_geom"]
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    gdf = gdf.to_crs(epsg=3857)

    # Apply the buffer
    gdf['geometry'] = gdf['geometry'].buffer(buffer_distance)
    
    # Convert back to EPSG:4326
    gdf = gdf.to_crs("EPSG:4326")
    return gdf