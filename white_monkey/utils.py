
# Helper functions - these should be defined before the UDF
def get_greenest_pixel(arr_rgbi, how="median", fillna=True):
    import numpy as np
    import pandas as pd
    
    # Calculate NDVI using last 2 bands (assuming B08 and B04 for NIR and Red)
    # But since you're only using RGB, this might need adjustment
    if arr_rgbi.shape[0] < 4:
        # If we don't have NIR band, use a simple approach
        # Just take the median across time for each band
        output_bands = []
        for b in range(min(3, arr_rgbi.shape[0])):
            band_data = arr_rgbi[b]
            if band_data.ndim == 3:  # (time, height, width)
                # Take median across time
                median_band = np.median(band_data, axis=0)
            else:
                median_band = band_data
            output_bands.append(median_band)
        return np.stack(output_bands)
    
    # Original NDVI-based approach for when we have NIR
    out = (arr_rgbi[-1] * 1.0 - arr_rgbi[-2] * 1.0) / (
        arr_rgbi[-1] * 1.0 + arr_rgbi[-2] * 1.0 + 1e-8  # Added small epsilon to avoid division by zero
    )
    t_len = out.shape[0]
    out_flat = out.reshape(out.shape[0], out.shape[1] * out.shape[2])
    
    # Find greenest pixels
    sorted_indices = np.argsort(out_flat, axis=0)
    if how == "median":
        median_index = sorted_indices[t_len // 2]
    elif how == "min":
        median_index = np.argmin(out_flat, axis=0)
    else:
        median_index = np.argmax(out_flat, axis=0)
    
    output_bands = []
    for b in [0, 1, 2]:  # RGB bands
        out_flat = arr_rgbi[b].reshape(t_len, out.shape[1] * out.shape[2])
        # Replace 0s with NaNs
        out_flat = np.where(out_flat == 0, np.nan, out_flat)
        if fillna:
            out_flat = pd.DataFrame(out_flat).ffill().bfill().values
        out_flat = out_flat[median_index, np.arange(out.shape[1] * out.shape[2])]
        output_bands.append(out_flat.reshape(out.shape[1], out.shape[2]))
    
    return np.stack(output_bands)


def get_arr(bounds, time_of_interest, output_shape, nth_item=None, max_items=100):
    """
    Get Sentinel-2 array with proper error handling
    """
    try:
        greenest_example_utils = fused.load('https://github.com/fusedio/udfs/tree/e74035a/public/Satellite_Greenest_Pixel').utils
        
        stac_items = greenest_example_utils.search_pc_catalog(
            bounds=bounds,
            time_of_interest=time_of_interest,
            query={"eo:cloud_cover": {"lt": 5}},
            collection="sentinel-2-l2a"
        )
        
        if not stac_items: 
            print("No STAC items found")
            return None
            
        # RGB bands only to avoid shape issues
        df_tiff_catalog = greenest_example_utils.create_tiffs_catalog(stac_items, ["B04", "B03", "B02"])
        
        print(f"Found {len(df_tiff_catalog)} images")
        
        if len(df_tiff_catalog) > max_items:
            print(f"Limiting to {max_items} items (found {len(df_tiff_catalog)})")
            df_tiff_catalog = df_tiff_catalog[:max_items]
        
        if nth_item is not None:
            if nth_item >= len(df_tiff_catalog):
                raise ValueError(f'{nth_item} >= total number of images ({len(df_tiff_catalog)})') 
            df_tiff_catalog = df_tiff_catalog[nth_item:nth_item + 1]
            arrs_out = greenest_example_utils.run_pool_tiffs(bounds, df_tiff_catalog, output_shape)
            arr = arrs_out.squeeze()
        else:
            arrs_out = greenest_example_utils.run_pool_tiffs(bounds, df_tiff_catalog, output_shape)
            arr = get_greenest_pixel(arrs_out, how='median', fillna=True)
        
        return arr
        
    except Exception as e:
        print(f"Error in get_arr: {e}")
        return None


# Main UDF
@fused.udf
def udf(bounds: fused.types.Bounds = None,
        chip_len: int = 512, 
        scale: float = 0.1,
        time_of_interest = "2021-11-01/2021-12-30",
        max_items: int = 50):
    import numpy as np
    
    # Get tiles
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/2f41ae1/public/common/").utils
    tile = common_utils.get_tiles(bounds, clip=True)
    
    # Get the satellite array
    arr = get_arr(tile, 
                  time_of_interest=time_of_interest, 
                  output_shape=(chip_len, chip_len), 
                  max_items=max_items)
    
    if arr is None:
        print("Failed to get array, returning black image")
        return np.zeros((3, chip_len, chip_len), dtype=np.uint8)
    
    print(f"Array shape: {arr.shape}")
    
    # Handle different array structures
    if arr.ndim == 4:  # (time, bands, height, width)
        arr = arr[0]  # Take first time step
    elif arr.ndim == 3 and arr.shape[0] > 3:  # More than 3 bands
        arr = arr[:3]  # Take first 3 bands
    
    # Apply scaling and clipping
    arr = np.clip(arr * scale, 0, 255).astype("uint8")
    
    # Ensure we have exactly 3 bands
    if arr.shape[0] < 3:
        # Duplicate bands to get 3 channels
        while arr.shape[0] < 3:
            arr = np.concatenate([arr, arr[-1:]], axis=0)
        arr = arr[:3]
    
    print(f"Final array shape: {arr.shape}")
    return arr