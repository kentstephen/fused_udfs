@fused.udf
def udf(bounds: fused.types.Bounds = [-83.9,14.7,-54.8,40.4], 
        time_slice: int =960, channel: str = 'swir', chip_len: int = 512):
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
        return np.zeros((chip_len, chip_len), dtype=np.uint8)
    
    row_min, row_max = rows.min(), rows.max()
    col_min, col_max = cols.min(), cols.max()
    
    # Get the data
    data = z[channel][time_slice, row_min:row_max+1, col_min:col_max+1]
    
    # Convert to float for processing
    data_float = data.astype(np.float32)
    
    # Use percentile-based stretching for better contrast (ignoring extreme outliers)
    p2, p98 = np.percentile(data_float, [2, 98])
    data_stretched = np.clip((data_float - p2) / (p98 - p2), 0, 1)
    
    #  # data_float = data.astype(np.float32)
    # p2, p98 = np.percentile(data_float, [2, 98])
    # data_stretched = np.clip((data_float - p2) / (p98 - p2), 0, 1)
    arr_uint8 = (data_stretched * 255).astype(np.uint8)
    
    return arr_uint8