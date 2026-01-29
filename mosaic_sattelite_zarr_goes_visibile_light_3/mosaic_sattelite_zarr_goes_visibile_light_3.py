@fused.udf
def udf(bounds: fused.types.Bounds = None, time_slice: int = -801, 
        red_channel='vis', green_channel='swir', blue_channel='lwir',
        chip_len: int = 256):
    import icechunk
    import zarr
    import numpy as np
    from scipy.ndimage import zoom
    
    storage = icechunk.s3_storage(
        bucket="us-west-2.opendata.source.coop",
        prefix="bkr/gmgi/gmgsi_v3.icechunk",
        region="us-west-2",
        anonymous=True
    )
    
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")
    store = session.store
    z = zarr.open_group(store=store, mode='r')
    
    lats = z['latitude'][:]
    lons = z['longitude'][:]
    
    minx, miny, maxx, maxy = bounds
    mask = (lats >= miny) & (lats <= maxy) & (lons >= minx) & (lons <= maxx)
    
    rows, cols = np.where(mask)
    if len(rows) == 0:
        return np.zeros((3, chip_len, chip_len), dtype=np.uint8), bounds
    
    row_min, row_max = rows.min(), rows.max()
    col_min, col_max = cols.min(), cols.max()
    
    # Function to process a single band
    def process_band(channel_name):
        data = z[channel_name][time_slice, row_min:row_max+1, col_min:col_max+1]
        data_float = data.astype(np.float32)
        
        # Use percentile normalization to reduce noise
        p2, p98 = np.percentile(data_float, [2, 98])
        normalized = np.clip((data_float - p2) / (p98 - p2), 0, 1)
        
        # Resample
        h, w = normalized.shape
        zoom_factors = (chip_len / h, chip_len / w)
        resampled = zoom(normalized, zoom_factors, order=2)
        
        return resampled
    
    # Process each band
    red = process_band(red_channel)
    green = process_band(green_channel)
    blue = process_band(blue_channel)
    
    # Stack as (3, height, width) - CHANNELS FIRST!
    rgb = np.stack([red, green, blue], axis=0)
    
    # Convert to uint8
    rgb_uint8 = (rgb * 255).astype(np.uint8)
    
    print(f"RGB shape (channels first): {rgb_uint8.shape}")
    
    return rgb_uint8, bounds