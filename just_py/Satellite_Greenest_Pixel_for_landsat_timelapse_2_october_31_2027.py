import json

# https://planetarycomputer.microsoft.com/dataset/modis-09A1-061
modis_params = json.dumps(
    {
        "collection": "modis-09A1-061",
        "band_list": [
            "sur_refl_b01",
            "sur_refl_b04",
            "sur_refl_b03",
            "sur_refl_b02",
        ],
        "time_of_interest": "2020-09/2020-10",
        "query": {},
        "scale": 0.1,
    }
)

# https://planetarycomputer.microsoft.com/dataset/landsat-c2-l2
landsat_params = json.dumps(
    {
        "collection": "landsat-c2-l2",
        "band_list": ["red", "green", "blue"],
        "time_of_interest": "2024-067-01/2024-09-30",
        "query": {"eo:cloud_cover": {"lt": 2}},
        "scale": 0.01,
    }
)
# https://planetarycomputer.microsoft.com/dataset/sentinel-2-l2a
sentinel_params = json.dumps(
    {
        "collection": "sentinel-2-l2a",
        "band_list": ["B02", "B03", "B04", "B08"],
        "time_of_interest": "2021-09-01/2021-09-30",
        "query": {"eo:cloud_cover": {"lt": 5}},
        "scale": 0.1,
    }
)


@fused.udf
def udf(
    bounds: fused.types.Bounds = [-122.463,37.755,-122.376,37.803],
    collection_params=sentinel_params,
    chip_len: int = 512,
    how: str = "max",  # median, min. default is max
    fillna: bool = False,  # This adresses stripes
):
    import json
    
    import fused
    import numpy as np
    import numpy.ma as ma
    import pandas as pd

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    tile = common.get_tiles(bounds, clip=True)

    collection, band_list, time_of_interest, query, scale = json.loads(
        collection_params
    ).values()

    print(collection, band_list, time_of_interest, query)

    stac_items = search_pc_catalog(
        bounds=tile, time_of_interest=time_of_interest, query=query, collection=collection
    )
    if not stac_items:
        return
    # max_items = 10  # adjust as needed
    # stac_items = stac_items[:max_items]
    print("Processing stac_items: ", len(stac_items))
    print(stac_items[0].assets.keys())
    df_tiff_catalog = create_tiffs_catalog(stac_items, band_list)
    
    output_shape = get_output_shape_from_bounds(bounds, chip_len)
    arrs_out = run_pool_tiffs(tile, df_tiff_catalog, output_shape=output_shape)
    # Generate arr with imagery
    arr = get_greenest_pixel(arrs_out, how=how, fillna=fillna)[:3,:,:]

    arr_scaled = arr * 1.0 * scale
    arr_scaled = np.clip(arr_scaled, 0, 255).astype("uint8")
    return arr_scaled, bounds
    # TODO: color correction https://custom-scripts.sentinel-hub.com/custom-scripts/sentinel-2/poor_mans_atcor/


@fused.cache
def run_pool_tiffs(bounds, df_tiffs, output_shape):
    import numpy as np

    columns = df_tiffs.columns

    def fn_read_tiff(tiff_url, bounds=bounds, output_shape=output_shape):
        common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
        return common.read_tiff(bounds, tiff_url, output_shape=output_shape)

    tiff_list = []
    for band in columns:
        for i in range(len(df_tiffs)):
            tiff_list.append(df_tiffs[band].iloc[i])

    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    arrs_tmp = common.run_pool(fn_read_tiff, tiff_list)
    
    # Debug: print shapes
    print("Shapes returned:", [arr.shape if arr is not None else None for arr in arrs_tmp])
    
    # Filter arrays to expected shape and handle None
    expected_shape = output_shape if len(output_shape) == 2 else output_shape[-2:]
    arrs_filtered = []
    for arr in arrs_tmp:
        if arr is not None and arr.shape == expected_shape:
            arrs_filtered.append(arr)
        elif arr is not None:
            # Resize to expected shape
            from skimage.transform import resize
            arr_resized = resize(arr, expected_shape, preserve_range=True, anti_aliasing=True)
            arrs_filtered.append(arr_resized)
    
    if not arrs_filtered:
        raise ValueError("No valid arrays returned from TIFF reading")
    
    arrs_out = np.stack(arrs_filtered)
    arrs_out = arrs_out.reshape(len(columns), len(df_tiffs), output_shape[-2], output_shape[-1])
    return arrs_out


def search_pc_catalog(
    bounds,
    time_of_interest,
    query={"eo:cloud_cover": {"lt": 5}},
    collection="sentinel-2-l2a",
):
    import planetary_computer
    import pystac_client

    # Instantiate PC client
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )

    # Search catalog
    items = catalog.search(
        collections=[collection],
        bbox=bounds.total_bounds,
        datetime=time_of_interest,
        query=query,
    ).item_collection()

    if len(items) == 0:
        print(f"empty for {time_of_interest}")
        return

    return items


def create_tiffs_catalog(stac_items, band_list):
    import pandas as pd

    input_paths = []
    for selected_item in stac_items:
        max_key_length = len(max(selected_item.assets, key=len))
        input_paths.append([selected_item.assets[band].href for band in band_list])
    return pd.DataFrame(input_paths, columns=band_list)


def get_greenest_pixel(arr_rgb, how="max", fillna=True):
    """
    Simplified function to select pixel values across time dimension.
    
    Args:
        arr_rgb: Array of shape (3, time, height, width) with RGB bands
        how: Method to select across time - "max", "median", or "min"
        fillna: Whether to fill NaN values
    
    Returns:
        Array of shape (3, height, width)
    """
    import numpy as np
    import pandas as pd
    
    output_bands = []
    
    for b in range(3):  # For each of the 3 RGB bands
        # Shape: (time, height, width)
        band_data = arr_rgb[b]
        t_len = band_data.shape[0]
        
        # Flatten spatial dimensions: (time, height*width)
        band_flat = band_data.reshape(t_len, band_data.shape[1] * band_data.shape[2])
        
        # Replace 0s with NaNs
        band_flat = np.where(band_flat == 0, np.nan, band_flat)
        
        if fillna:
            band_flat = pd.DataFrame(band_flat).ffill().bfill().values
        
        # Select across time dimension
        if how == "median":
            result = np.nanmedian(band_flat, axis=0)
        elif how == "min":
            result = np.nanmin(band_flat, axis=0)
        else:  # max
            result = np.nanmax(band_flat, axis=0)
        
        # Reshape back to (height, width)
        output_bands.append(result.reshape(band_data.shape[1], band_data.shape[2]))
    
    return np.stack(output_bands)

def get_output_shape_from_bounds(bounds, base_size=512):
  """Calculate output shape preserving aspect ratio."""
  min_x, min_y, max_x, max_y = bounds
  width = max_x - min_x
  height = max_y - min_y
  aspect_ratio = width / height

  if aspect_ratio > 1:  # wider than tall
      return (int(base_size / aspect_ratio), base_size)
  else:  # taller than wide
      return (base_size, int(base_size * aspect_ratio))