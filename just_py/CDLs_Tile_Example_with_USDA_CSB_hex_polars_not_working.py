common = fused.load("https://github.com/fusedio/udfs/tree/b3a7ff8/public/common/").utils
nlcd_example_utils = fused.load('https://github.com/fusedio/udfs/tree/1b2b7e3/public/NLCD_Tile_Example/').utils

@fused.udf
def udf(
    bounds: fused.types.Bounds = [-121.525, 37.70, -120.96, 38.06],
    year: int = 2024,
    crop_type: str = "",
    chip_len: int = 256,
    colored: bool = True,
    apply_csb_mask = True,
    res: int = 10,
):
    import numpy as np
    import pandas as pd
    
    # Convert bounds to tile - only once
    tile = common.get_tiles(bounds, clip=True)

    x, y, z = tile.iloc[0][["x", "y", "z"]]
    res_offset = 1  # lower makes the hex finer
    res = max(min(int(3 + z / 1.5), 12) - res_offset, 2)
    print(res)
    # Read TIFF data with caching
    input_tiff_path = f"s3://fused-asset/data/cdls/{year}_30m_cdls.tif"
    data = common.read_tiff(
        tile, input_tiff_path, output_shape=(chip_len, chip_len), 
        return_colormap=True, cache_max_age="90d"
    )
    
    if not data:
        print("no data")
        return None
        
    arr_int, metadata = data
    
    # Apply CSB mask if requested
    if apply_csb_mask:
        arr_int = get_csb_mask(tile, bounds, arr_int, chip_len)
        if arr_int is None:
            return None
    
    # Filter crops early to reduce data size
    if crop_type:
        arr_int = filter_crops(arr_int, crop_type, verbose=False)

    if not colored:
        return arr_int, bounds
    
    # Only process hex conversion if we're returning colored results
    colormap = metadata["colormap"]
    
    # Convert to H3 hexes
    df = common.arr_to_h3(arr_int, bounds, res=res, ordered=False)
    
    # Vectorized operations for better performance
    df = process_hex_data(df, colormap)
    
    if len(df) == 0:
        print(f"Empty dataframe for tile {tile}")
        return None
        
    # Print stats
    print(crop_counts(arr_int).head(20))
    
    return df


# Move cached functions outside main UDF for better caching
@fused.cache
def get_csb_mask(tile, bounds, arr_int, chip_len):
    """Get and apply CSB mask to the array"""
    import numpy as np
    try:
        # Get crop boundaries
        crop_boundaries = fused.run("fsh_6cOfhwzXwxYj3d8rTjJaDD", bounds=tile)
        
        if crop_boundaries is not None and len(crop_boundaries) > 0:
            # Create and apply mask
            mask = create_polygon_mask(crop_boundaries, bounds, chip_len)
            print(f"Applied mask from {len(crop_boundaries)} polygons")
            return np.where(mask, arr_int, 0)
        else:
            print("No crop boundaries found for masking")
            return arr_int
            
    except Exception as e:
        print(f"Error applying polygon mask: {e}")
        return arr_int


def process_hex_data(df, colormap):
    """Efficiently process hex data with vectorized operations using Polars"""
    import polars as pl
    import numpy as np
    
    # Convert pandas DataFrame to Polars
    df = pl.from_pandas(df)
    
    # Find most frequent value per hex using map_elements
    def get_most_frequent(agg_data):
        unique_vals, counts = np.unique(agg_data, return_counts=True)
        max_idx = np.argmax(counts)
        return {
            "most_freq": int(unique_vals[max_idx]), 
            "n_pixel": int(counts[max_idx])
        }
    
    # Apply vectorized operation
    df = df.with_columns([
        pl.col('agg_data').map_elements(
            get_most_frequent,
            return_dtype=pl.Struct([
                pl.Field("most_freq", pl.Int64),
                pl.Field("n_pixel", pl.Int64)
            ])
        ).alias("freq_result")
    ]).unnest("freq_result")
    
    # Filter out zero values
    df = df.filter(pl.col('most_freq') > 0)
    
    if df.height == 0:
        return df
    
    # Vectorized color mapping
    df = df.with_columns([
        pl.col('most_freq').map_elements(
            lambda x: colormap.get(x, [0, 0, 0, 0]),
            return_dtype=pl.List(pl.UInt8)
        ).alias("rgba_list")
    ]).with_columns([
        pl.col("rgba_list").list.get(0).alias("r"),
        pl.col("rgba_list").list.get(1).alias("g"), 
        pl.col("rgba_list").list.get(2).alias("b"),
        pl.col("rgba_list").list.get(3).alias("a")
    ])
    
    # Map crop types
    df = df.with_columns([
        pl.col('most_freq').map_elements(
            int_to_crop,
            return_dtype=pl.String
        ).alias('crop_type')
    ])
    
    # Create hex color strings
    df = df.with_columns([
        pl.col('most_freq').map_elements(
            lambda x: nlcd_example_utils.rgb_to_hex(colormap[x]) if x in colormap else "NaN",
            return_dtype=pl.String
        ).alias('color')
    ])
    
    # Return only needed columns
    df = df.select(['hex', 'crop_type', 'color', 'r', 'g', 'b', 'a', 'most_freq', 'n_pixel'])

    return df.to_pandas()

def create_polygon_mask(gdf, bounds, chip_len):
    """Create a boolean mask from polygon geometries"""
    import numpy as np
    from rasterio.features import rasterize
    from rasterio.transform import from_bounds
    
    # Create transform for the raster
    transform = from_bounds(bounds[0], bounds[1], bounds[2], bounds[3], chip_len, chip_len)
    
    # Rasterize the polygons to create a mask
    mask = rasterize(
        gdf.geometry,
        out_shape=(chip_len, chip_len),
        transform=transform,
        fill=0,  # Background value (outside polygons)
        default_value=1,  # Value inside polygons
        dtype=np.uint8
    )
    
    return mask.astype(bool)


# Keep other functions unchanged but optimize where possible
@fused.cache
def crop_to_int(crop_type, verbose=True):
    import pandas as pd
    import requests

    url = "https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json"
    df_meta = pd.DataFrame(requests.get(url).json()["summaries"]["eo:bands"][0]["gee:classes"])
    mask = df_meta.description.map(lambda x: crop_type.lower() in x.lower())
    df_meta_masked = df_meta[mask]
    if verbose:
        print(f"{df_meta_masked=}")
        print("crop type options", df_meta.description.values)
    return df_meta_masked.value.values


@fused.cache
def int_to_crop(val):
    import requests
    import pandas as pd

    url = "https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json"
    df_meta = pd.DataFrame(requests.get(url).json()["summaries"]["eo:bands"][0]["gee:classes"]).set_index("value")
    return df_meta.loc[val].description


@fused.cache
def crop_type_list(crop_type):
    if not crop_type:
        return []
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
        return arr
    else:
        if verbose:
            print(f"{crop_type=} was not found in the list of crops")
        return arr


def crop_counts(arr):
    import numpy as np
    import pandas as pd
    import requests

    unique_values, counts = np.unique(arr, return_counts=True)
    df = pd.DataFrame(counts, index=unique_values, columns=["n_pixel"]).sort_values(by="n_pixel", ascending=False)
    url = "https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json"
    df_meta = pd.DataFrame(requests.get(url).json()["summaries"]["eo:bands"][0]["gee:classes"])
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