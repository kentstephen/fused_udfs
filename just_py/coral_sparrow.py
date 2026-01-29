@fused.udf
def udf(bounds: fused.types.Bounds = [-80, 16, -74, 20], time_slice: int = -1):
    import icechunk
    import zarr
    import numpy as np
    
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
    
    # Check what variables are available
    print("Available arrays:", list(z.keys()))
    print("Vis array info:", z['vis'].info)
    
    lats = z['latitude'][:]
    lons = z['longitude'][:]
    
    minx, miny, maxx, maxy = bounds
    mask = (lats >= miny) & (lats <= maxy) & (lons >= minx) & (lons <= maxx)
    
    rows, cols = np.where(mask)
    if len(rows) == 0:
        print("No data found in bounds!")
        return np.zeros((512, 512), dtype=np.uint8), bounds
    
    row_min, row_max = rows.min(), rows.max()
    col_min, col_max = cols.min(), cols.max()
    
    print(f"Extracting rows {row_min}:{row_max+1}, cols {col_min}:{col_max+1}")
    
    # Get the data
    vis_data = z['vis'][time_slice, row_min:row_max+1, col_min:col_max+1]
    
    # Debug the values
    print(f"Data shape: {vis_data.shape}")
    print(f"Data type: {vis_data.dtype}")
    print(f"Min value: {np.nanmin(vis_data)}")
    print(f"Max value: {np.nanmax(vis_data)}")
    print(f"Mean value: {np.nanmean(vis_data)}")
    print(f"Number of NaNs: {np.isnan(vis_data).sum()}")
    print(f"Number of zeros: {(vis_data == 0).sum()}")
    print(f"Unique values sample:", np.unique(vis_data.flatten())[:10])
    
    subset_lats = lats[row_min:row_max+1, col_min:col_max+1]
    subset_lons = lons[row_min:row_max+1, col_min:col_max+1]
    
    # Try different normalization
    vis_clean = vis_data.copy()
    # Replace NaN and negative values with 0
    vis_clean = np.where(np.isnan(vis_clean) | (vis_clean < 0), 0, vis_clean)
    
    # Check if we have any valid data
    if vis_clean.max() > vis_clean.min():
        # Use percentile-based scaling to avoid outliers
        p2, p98 = np.percentile(vis_clean[vis_clean > 0], [2, 98])
        vis_scaled = np.clip((vis_clean - p2) / (p98 - p2), 0, 1)
        arr_uint8 = (vis_scaled * 255).astype(np.uint8)
    else:
        print("WARNING: No valid data range found!")
        arr_uint8 = np.zeros_like(vis_clean, dtype=np.uint8)
    
    actual_bounds = [
        float(subset_lons.min()),
        float(subset_lats.min()),
        float(subset_lons.max()),
        float(subset_lats.max())
    ]
    
    return arr_uint8, actual_bounds