@fused.udf
def udf(bounds: fused.types.Bounds = None, buffer_multiple: float = 1):
    # Using common fused functions as helper
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    import xarray as xr
    import ee
    
    bounds_gdf = utils.bounds_to_gdf(bounds)
    total_bounds = bounds_gdf.bounds.values[0]
    
    # Set your own creds
    utils.ee_initialize(service_account_name='fused-nyt-gee@fused-nyt.iam.gserviceaccount.com', key_path="/mount/gee_key.json")
    
    geom = ee.Geometry.Rectangle(*total_bounds)
    ic = ee.Image("projects/sat-io/open-datasets/CTREES/AMAZON-CANOPY-TREE-HT")
    
    # Fix the bounds.z access - bounds is likely [xmin, ymin, xmax, ymax, zoom]
    # or you might need to get zoom differently
    zoom_level = bounds[4] if len(bounds) > 4 else 10  # Default zoom if not available
    
    ds = xr.open_dataset(
        ic,
        engine='ee',
        geometry=geom,
        scale=1/2**max(0, zoom_level)
    )
    
    # Fix the missing assignment/continuation
    ds = ds.max(dim='time')
    
    return ds