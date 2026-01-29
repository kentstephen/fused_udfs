visualize = fused.load(
    "https://github.com/fusedio/udfs/tree/2b25cb3/public/common/"
).utils.visualize
@fused.cache
def get_usa_boundary(bbox):
    import geopandas as gpd
    
    # Use nation boundary instead of tracts for simpler masking
    gdf = gpd.read_file('https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_nation_5m.zip')
    
    # Convert to 4326 and clip to bbox
    gdf_clipped = gdf.to_crs(4326).clip(bbox)
    
    # No need to concat with bbox
    return gdf_clipped

# # Then when using it:
# gdf_w_bbox = get_usa_boundary(bbox)
# geom_mask = fused.utils.common.gdf_to_mask_arr(gdf_w_bbox, arr.shape, first_n=1)
# arr = np.ma.masked_array(arr, mask=geom_mask)