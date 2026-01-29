@fused.udf
def udf(bounds : fused.types.Bounds=[-132.3, -40.9, 34.1, 75.1], time_slice: int = -400):
    import icechunk
    import zarr
    import numpy as np
    # from scipy.ndimage import zoom
    
    storage = icechunk.s3_storage(
        bucket="us-west-2.opendata.source.coop",
        prefix="bkr/gmgi/gmgsi_v3.icechunk",
        region="us-west-2",
        anonymous=True
    )
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    # zoom = common.estimate_zoom(bounds)
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")
    store = session.store
    z = zarr.open_group(store=store, mode='r')
    
    # Check the time dimension to understand what times we have
    if 'time' in z:
        times = z['time'][:]
        print(f"Time range: {times[0]} to {times[-1]}")
        print(f"Total timesteps: {len(times)}")
        print(f"Using timestep {time_slice}: {times[time_slice]}")
    
    lats = z['latitude'][:]
    lons = z['longitude'][:]
    
    minx, miny, maxx, maxy = bounds
    mask = (lats >= miny) & (lats <= maxy) & (lons >= minx) & (lons <= maxx)
    
    rows, cols = np.where(mask)
    if len(rows) == 0:
        return np.zeros((chip_len, chip_len), dtype=np.uint8)
    
    row_min, row_max = rows.min(), rows.max()
    col_min, col_max = cols.min(), cols.max()
    
    print(f"Region: rows {row_min}:{row_max+1}, cols {col_min}:{col_max+1}")
    
    # Get the data
    vis_data = z['vis'][time_slice, row_min:row_max+1, col_min:col_max+1]
    
    print(f"Data stats - min: {vis_data.min()}, max: {vis_data.max()}, mean: {vis_data.mean():.2f}")
    
    subset_lats = lats[row_min:row_max+1, col_min:col_max+1]
    subset_lons = lons[row_min:row_max+1, col_min:col_max+1]
    
    # Since values are 0-17, let's stretch them to use full 0-255 range
    vis_clean = vis_data.astype(np.float32)
    
    # Simple linear stretch of existing values
    if vis_clean.max() > 0:
        arr_uint8 = ((vis_clean / vis_clean.max()) * 255).astype(np.uint8)
    else:
        arr_uint8 = np.zeros_like(vis_clean, dtype=np.uint8)
    
    # Resample to chip_len x chip_len
    # zoom_factors = (chip_len / arr_uint8.shape[0], chip_len / arr_uint8.shape[1])
    # arr_resampled = zoom(arr_uint8, zoom_factors, order=1).astype(np.uint8)
    
    return arr_uint8