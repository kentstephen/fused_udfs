@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    time_slice: int = 960,
    cloud_min: float = 0.6,
    cloud_max: float = 1.0,
    return_mask: bool = True,
):
    import icechunk
    import zarr
    import numpy as np
    import cv2
    
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
    
    # Get spatial subset
    lats = z['latitude'][:]
    lons = z['longitude'][:]
    
    minx, miny, maxx, maxy = bounds
    mask = (lats >= miny) & (lats <= maxy) & (lons >= minx) & (lons <= maxx)
    
    rows, cols = np.where(mask)
    if len(rows) == 0:
        return np.zeros((256, 256), dtype=np.uint8)
    
    row_min, row_max = rows.min(), rows.max()
    col_min, col_max = cols.min(), cols.max()
    
    # Get SWIR data
    swir_data = z['swir'][time_slice, row_min:row_max+1, col_min:col_max+1]
    
    if return_mask:
        # Normalize SWIR to 0-1 range
        swir_normalized = swir_data.astype(np.float32)
        if swir_normalized.max() > 0:
            swir_normalized = swir_normalized / swir_normalized.max()
        
        # Simple threshold for clouds (high SWIR values = clouds)
        cloud_mask = np.zeros_like(swir_normalized)
        cloud_mask[(swir_normalized >= cloud_min) & (swir_normalized <= cloud_max)] = 1
        
        # Morphological operations to clean up mask
        kernel_size = 5
        kernel = np.ones((kernel_size, kernel_size), np.uint8)
        
        # Remove small noise
        cloud_mask_clean = cv2.morphologyEx(
            cloud_mask.astype(np.uint8), 
            cv2.MORPH_OPEN, 
            kernel
        )
        
        # Fill small holes
        cloud_mask_clean = cv2.morphologyEx(
            cloud_mask_clean, 
            cv2.MORPH_CLOSE, 
            kernel
        )
        
        # Optional: Gaussian blur for smoother edges
        cloud_mask_smooth = cv2.GaussianBlur(
            cloud_mask_clean.astype(np.float32), 
            (kernel_size, kernel_size), 
            1
        )
        
        # Convert to RGBA for visualization
        mask_uint8 = (cloud_mask_smooth * 255).astype(np.uint8)
        
        # Return as RGBA - cyan/blue color (R=0, G=255, B=255)
        return np.stack([
            mask_uint8,      # Red = 0
            mask_uint8*0,          # Green = full
            mask_uint8,          # Blue = full
            mask_uint8 // 2      # Alpha (semi-transparent)
        ])
    else:
        # # Just return the stretched SWIR data
        # swir_clean = swir_data.astype(np.float32)
        # if swir_clean.max() > 0:
        #     arr_uint8 = ((swir_clean / swir_clean.max()) * 255).astype(np.uint8)
        # else:
        #     arr_uint8 = np.zeros_like(swir_clean, dtype=np.uint8)
        
        # return arr_uint8
        return None