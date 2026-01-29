@fused.udf
def udf(bounds: fused.types.Bounds = [-130, 25, -60, 60],
        time_index: int = 0,
        variable: str = 'PM25_RH35_GCC'):
    import icechunk
    import zarr
    import numpy as np
    
    storage = icechunk.s3_storage(
        bucket="us-west-2.opendata.source.coop",
        prefix="bkr/geos/geos_15min.icechunk",
        region="us-west-2",
        anonymous=True
    )
    
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")
    store = session.store
    z = zarr.open_group(store=store, mode='r')
    
    # Get coordinate arrays
    lats = z['latitude'][:]
    lons = z['longitude'][:]
    
    minx, miny, maxx, maxy = bounds
    print(z)
    
    # Find lat/lon indices for bounds
    lat_mask = (lats >= miny) & (lats <= maxy)
    lon_mask = (lons >= minx) & (lons <= maxx)
    
    lat_indices = np.where(lat_mask)[0]
    lon_indices = np.where(lon_mask)[0]
    
    if len(lat_indices) == 0 or len(lon_indices) == 0:
        print("No data in bounds")
        return
    
    lat_min, lat_max = lat_indices.min(), lat_indices.max()
    lon_min, lon_max = lon_indices.min(), lon_indices.max()
    
    # Get the data - shape is (time, lat, lon)
    data = z[variable][time_index, lat_min:lat_max+1, lon_min:lon_max+1]
    
    # Convert to float for processing
    data_float = data.astype(np.float32)
    
    # Handle NaN/missing values
    valid_mask = ~np.isnan(data_float)
    if not valid_mask.any():
        print("No valid data")
        return
    
    # Use percentile-based stretching on valid data only
    valid_data = data_float[valid_mask]
    p2, p98 = np.percentile(valid_data, [2, 98])
    data_stretched = np.clip((data_float - p2) / (p98 - p2), 0, 1)
    
    # Set NaN to 0 after stretching
    data_stretched[~valid_mask] = 0
    
    arr_uint8 = (data_stretched * 255).astype(np.uint8)
    
    return arr_uint8