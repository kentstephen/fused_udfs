import geopandas as gpd
@fused.cache
def get_gdf():
    gdf = gpd.read_file('https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/MHC_Inventory_SHP.zip')
    gdf = gdf.to_crs("EPSG:4326")  # Assign the transformed GeoDataFrame back to gdf
    # gdf["geometry"] = gdf.geometry.centroid  # Update the geometry to centroids
    return gdf
