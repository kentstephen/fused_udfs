import geopandas as gpd
import shapely

@fused.cache
def get_mask():
    # Load NYC boroughs boundary file
    url = 'https://data.cityofnewyork.us/api/geospatial/j2bc-fus8?method=export&format=GeoJSON'
    gdf_nyc = gpd.read_file(url, driver='GeoJSON')
    
    # Create mask by dissolving all boroughs into single polygon
    # Only keep the geometry column
    return gdf_nyc.dissolve()[['geometry']]
@fused.cache
def nyc_mask(gdf_overture):
    """
    Masks (clips) input geodataframe to NYC boundaries, using a bounding box pre-filter 
    for better performance.
    
    Parameters:
    -----------
    gdf_overture : GeoDataFrame
        The geodataframe to be masked/clipped to NYC boundaries
        
    Returns:
    --------
    GeoDataFrame
        Input geodataframe clipped to NYC boundaries
    """
    # Define NYC bounding box
    bbox = gpd.GeoDataFrame(
        geometry=[shapely.box(-74.258843, 40.476578, -73.700233, 40.91763)],
        crs=4326
    )
    
    # Ensure input data is in correct CRS
    if gdf_overture.crs != "EPSG:4326":
        gdf_overture = gdf_overture.to_crs("EPSG:4326")
    
    # First filter by bounding box for speed
    gdf_bbox_filtered = gpd.overlay(gdf_overture, bbox, how='intersection')
    
    # Get NYC mask
    mask = get_mask()
    if mask.crs != "EPSG:4326":
        mask = mask.to_crs("EPSG:4326")
    
    # Perform final intersection with precise NYC boundary
    result = gpd.overlay(gdf_bbox_filtered, mask, how='intersection')
    
    # Drop any columns that came from the mask
    mask_columns = mask.columns.drop('geometry')
    if not mask_columns.empty:
        result = result.drop(columns=mask_columns)
        
    return result

 #  h3_cell_to_lat(hex) cell_lat,
 #               h3_cell_to_lng(hex) cell_lng,
            
 # WHERE cell_lat >= {ymin}
 #        AND cell_lat < {ymax}
 #        AND cell_lng >= {xmin}
 #        AND cell_lng < {xmax}
    