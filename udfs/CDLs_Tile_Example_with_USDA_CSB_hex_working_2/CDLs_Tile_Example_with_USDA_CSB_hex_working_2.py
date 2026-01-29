common = fused.load("https://github.com/fusedio/udfs/tree/b3a7ff8/public/common/").utils
nlcd_example_utils = fused.load('https://github.com/fusedio/udfs/tree/1b2b7e3/public/NLCD_Tile_Example/').utils

# Cache the metadata globally to avoid repeated requests
@fused.cache
def get_cdl_metadata():
    import pandas as pd
    import requests
    url = "https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json"
    return pd.DataFrame(requests.get(url).json()["summaries"]["eo:bands"][0]["gee:classes"])

@fused.cache
def get_mask(bounds_tuple, chip_len):
    """Cached function to get polygon mask"""
    import numpy as np
    from rasterio.features import rasterize
    from rasterio.transform import from_bounds
    
    try:
        bounds = list(bounds_tuple)
        crop_boundaries = fused.run("fsh_6cOfhwzXwxYj3d8rTjJaDD", bounds=bounds)
        
        # Debug: Check if we got boundaries
        print(f"Crop boundaries type: {type(crop_boundaries)}")
        print(f"Crop boundaries length: {len(crop_boundaries) if crop_boundaries is not None else 'None'}")
        
        if crop_boundaries is not None and len(crop_boundaries) > 0:
            transform = from_bounds(bounds[0], bounds[1], bounds[2], bounds[3], chip_len, chip_len)
            
            mask = rasterize(
                crop_boundaries.geometry,
                out_shape=(chip_len, chip_len),
                transform=transform,
                fill=0,
                default_value=1,
                dtype=np.uint8
            )
            
            # Debug: Check mask statistics
            mask_bool = mask.astype(bool)
            print(f"Mask shape: {mask_bool.shape}")
            print(f"Mask True pixels: {np.sum(mask_bool)} ({np.sum(mask_bool) / mask_bool.size * 100:.1f}%)")
            print(f"Mask False pixels: {np.sum(~mask_bool)} ({np.sum(~mask_bool) / mask_bool.size * 100:.1f}%)")
            
            return mask_bool
        else:
            print("No crop boundaries found for masking")
            return None
    except Exception as e:
        print(f"Error applying polygon mask: {e}")
        import traceback
        traceback.print_exc()
        return None

@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    year: int = 2024,
    crop_type: str = "",
    chip_len: int = 256,
    colored: bool = True,
    apply_csb_mask: bool = True,
    res: int = 10,
):
    import numpy as np
    import pandas as pd
    
    # Convert bounds to tile
    tile = common.get_tiles(bounds, clip=True)
    # using the following res formula can help
    # x, y, z = tile.iloc[0][["x", "y", "z"]]
    # res_offset = 1
    # res = max(min(int(3 + z / 1.5), 12) - res_offset, 2)
    # print(f"Resolution: {res}")
    
    input_tiff_path = f"s3://fused-asset/data/cdls/{year}_30m_cdls.tif"
    data = common.read_tiff(
        tile, input_tiff_path, output_shape=(chip_len, chip_len), return_colormap=True, cache_max_age="90d"
    )
    
    if data:
        arr_int, metadata = data
    else:
        print("no data")
        return None
    
    # Debug: Check data before masking
    print(f"Data shape before masking: {arr_int.shape}")
    print(f"Non-zero pixels before masking: {np.count_nonzero(arr_int)} ({np.count_nonzero(arr_int) / arr_int.size * 100:.1f}%)")
    unique_before = np.unique(arr_int)
    print(f"Unique values before masking: {len(unique_before)} classes")
    
    # Apply crop mask if requested
    if apply_csb_mask:
        bounds_tuple = tuple(bounds)
        mask = get_mask(bounds_tuple, chip_len)
        
        if mask is not None:
            # Debug: Verify mask dimensions match
            if mask.shape != arr_int.shape:
                print(f"WARNING: Mask shape {mask.shape} doesn't match data shape {arr_int.shape}")
                return None
            
            # Apply mask: keep values where mask is True, set to 0 where False
            arr_int_masked = np.where(mask, arr_int, 0)
            
            # Debug: Check masking effect
            print(f"\nAfter masking:")
            print(f"Non-zero pixels after masking: {np.count_nonzero(arr_int_masked)} ({np.count_nonzero(arr_int_masked) / arr_int_masked.size * 100:.1f}%)")
            unique_after = np.unique(arr_int_masked)
            print(f"Unique values after masking: {len(unique_after)} classes")
            
            # Calculate pixels that were masked out
            pixels_masked = np.count_nonzero(arr_int) - np.count_nonzero(arr_int_masked)
            print(f"Pixels masked out: {pixels_masked} ({pixels_masked / arr_int.size * 100:.1f}%)")
            
            arr_int = arr_int_masked
        else:
            print("No crop boundaries found - masking all data to zero")
            arr_int = np.zeros_like(arr_int)  
    # Filter crops if crop_type is specified
    if crop_type:
        arr_int = filter_crops(arr_int, crop_type, verbose=False)

    # Print out the top 20 classes
    print("\nTop 20 crop classes:")
    print(crop_counts(arr_int).head(20))
    
    colormap = metadata["colormap"]
    
    if colored:
        df = common.arr_to_h3(arr_int, bounds, res=res, ordered=False)

        if len(df) == 0:
            print(f"Empty dataframe for tile {tile}")
            return None

        # Vectorized operations for better performance
        agg_stats = df.agg_data.apply(lambda x: pd.Series({
            'most_freq': np.unique(x, return_counts=True)[0][np.argmax(np.unique(x, return_counts=True)[1])],
            'n_pixel': np.unique(x, return_counts=True)[1].max()
        }))
        
        df['most_freq'] = agg_stats['most_freq']
        df['n_pixel'] = agg_stats['n_pixel']
        df = df[df['most_freq'] > 0]
        
        if len(df) == 0:
            print(f"No valid crop data found for tile {tile}")
            return None
    
        # Vectorized color mapping
        df['color_tuple'] = df.most_freq.map(lambda x: colormap.get(x, [0,0,0,0]))
        color_df = pd.DataFrame(df['color_tuple'].tolist(), columns=['r', 'g', 'b', 'a'], index=df.index)
        df = pd.concat([df, color_df], axis=1)
        
        # Create lookup dict for crop types to avoid repeated function calls
        unique_vals = df['most_freq'].unique()
        crop_type_lookup = {val: int_to_crop(val) for val in unique_vals}
        df['crop_type'] = df['most_freq'].map(crop_type_lookup)
        
        # Vectorized hex color conversion
        df['color'] = df['color_tuple'].map(lambda x: nlcd_example_utils.rgb_to_hex(x) if len(x) > 0 else "NaN")
        
        # Drop intermediate columns
        df = df[['hex', 'crop_type', 'color', 'r', 'g', 'b', 'a', 'most_freq', 'n_pixel']]

        return df
    else:
        # Return both masked array and original for comparison if needed
        return arr_int, bounds

# Rest of the helper functions remain the same...
@fused.cache
def crop_to_int(crop_type, verbose=True):
    df_meta = get_cdl_metadata()
    mask = df_meta.description.str.lower().str.contains(crop_type.lower())
    df_meta_masked = df_meta[mask]
    if verbose:
        print(f"Matched crops: {df_meta_masked}")
        print("Available crop types:", df_meta.description.values)
    return df_meta_masked.value.values

@fused.cache
def int_to_crop(val):
    df_meta = get_cdl_metadata().set_index("value")
    try:
        return df_meta.loc[val].description
    except KeyError:
        return f"Unknown crop type ({val})"

@fused.cache
def crop_type_list(crop_type):
    try:
        vals = [int(i) for i in crop_type.split(',')]
    except:
        vals = crop_to_int(crop_type, verbose=False)
    return vals

def filter_crops(arr, crop_type, verbose=True):
    import numpy as np
    
    values_to_keep = crop_type_list(crop_type)
    if len(values_to_keep) > 0:
        mask = np.isin(arr, values_to_keep, invert=True)
        arr[mask] = 0
        if verbose:
            print(f"Filtered to keep crop types: {values_to_keep}")
        return arr
    else:
        print(f"Crop type '{crop_type}' was not found in the list of crops")
        return arr

def crop_counts(arr):
    import numpy as np
    import pandas as pd
    
    unique_values, counts = np.unique(arr, return_counts=True)
    df = pd.DataFrame(counts, index=unique_values, columns=["n_pixel"]).sort_values(by="n_pixel", ascending=False)
    
    df_meta = get_cdl_metadata()
    df = df_meta.set_index("value").join(df)[["description", "color", "n_pixel"]]
    df["color"] = "#" + df["color"]
    df.columns = ["crop_type", "color", "n_pixel"]
    df = df.sort_values("n_pixel", ascending=False)
    return df.dropna()

def crop_stats(df, n=100):
    stats = (
        df.groupby("data")
        .area.sum()
        .map(lambda x: round(x * 0.000247105, 2))  # converting m2 to acre
        .sort_values(ascending=False)
        .to_frame("area (Acre)")
    )
    stats["name"] = stats.index.map(int_to_crop)
    return stats.head(n)