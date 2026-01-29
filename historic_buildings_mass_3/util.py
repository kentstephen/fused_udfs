import geopandas as gpd
@fused.cache
def get_gdf():
    gdf = gpd.read_file('https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/MHC_Inventory_SHP.zip')
    gdf.to_crs("EPSG:4326")
    return gdf