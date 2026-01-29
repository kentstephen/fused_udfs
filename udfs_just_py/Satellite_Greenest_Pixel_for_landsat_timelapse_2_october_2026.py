# Define the Landsat parameters (must be defined before the UDF)
import json

landsat_params = json.dumps(
    {
        "collection": "landsat-c2-l2",
        "band_list": ["red", "green", "blue"],  # Landsat band names
        "time_of_interest": "1987-12-01/1987-12-31",
        "query": {"eo:cloud_cover": {"lt": 20}},
        "scale": 0.01,
    }
)

@fused.udf
def udf(
    bounds: fused.types.Bounds = [-122.463, 37.755, -122.376, 37.803],
    collection_params=landsat_params,
    chip_len: int = 512,
    how: str = "max",  # median, min. default is max
    fillna: bool = False,  # This adresses stripes
):
    import json
    import fused
    import numpy as np
    import pandas as pd  # kept for compatibility

    # Load common utilities once
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    # Convert bounds to tile
    tile = common.get_tiles(bounds, clip=True)

    # Parse collection parameters
    collection, band_list, time_of_interest, query, scale = json.loads(
        collection_params
    ).values()

    print(collection, band_list, time_of_interest, query)

    stac_items = search_pc_catalog(
        bounds=tile, time_of_interest=time_of_interest, query=query, collection=collection
    )
    if not stac_items:
        return
    max_items = 10  # adjust as needed
    stac_items = stac_items[:max_items]
    print("Processing stac_items: ", len(stac_items))
    print(stac_items[0].assets.keys())
    df_tiff_catalog = create_tiffs_catalog(stac_items, band_list)

    arrs_out = run_pool_tiffs(tile, df_tiff_catalog, output_shape=(chip_len, chip_len))

    # Composite across time
    arr = get_greenest_pixel(arrs_out, how=how, fillna=fillna)[:3, :, :]

    arr_scaled = arr * 1.0 * scale
    arr_scaled = np.clip(arr_scaled, 0, 255).astype("uint8")
    return arr_scaled, bounds
    # TODO: color correction https://custom-scripts.sentinel-hub.com/custom-scripts/sentinel-2/poor_mans_atcor/


@fused.cache
def run_pool_tiffs(bounds, df_tiffs, output_shape):
    import numpy as np
    import fused

    # Load common once for the pool workers
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    def fn_read_tiff(tiff_url, bounds=bounds, output_shape=output_shape, common=common):
        return common.read_tiff(bounds, tiff_url, output_shape=output_shape)

    # Build list of all tiff URLs (band-major, then time-major)
    tiff_list = [
        df_tiffs[band].iloc[i]
        for band in df_tiffs.columns
        for i in range(len(df_tiffs))
    ]

    # Load all tiffs in parallel
    arrs_tmp = common.run_pool(fn_read_tiff, tiff_list)
    arrs_out = np.stack(arrs_tmp)
    arrs_out = arrs_out.reshape(
        len(df_tiffs.columns), len(df_tiffs), output_shape[-2], output_shape[-1]
    )
    return arrs_out


def search_pc_catalog(
    bounds,
    time_of_interest,
    query={"eo:cloud_cover": {"lt": 5}},
    collection="sentinel-2-l2a",
):
    import planetary_computer
    import pystac_client

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )

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

    input_paths = [
        [selected_item.assets[band].href for band in band_list]
        for selected_item in stac_items
    ]
    return pd.DataFrame(input_paths, columns=band_list)


def get_greenest_pixel(arr_rgb, how="max", fillna=True):
    """
    Select pixel values across the time dimension.

    Args:
        arr_rgb: (3, time, height, width) array
        how: "max", "median", or "min"
        fillna: fill missing values

    Returns:
        (3, height, width) array
    """
    import numpy as np

    out = np.empty((3, arr_rgb.shape[2], arr_rgb.shape[3]), dtype=arr_rgb.dtype)

    for b in range(3):
        band_data = arr_rgb[b]                     # (time, h, w)
        flat = band_data.reshape(band_data.shape[0], -1)  # (time, h*w)

        # Replace 0 with NaN (sentinel/landsat use 0 for no-data)
        flat = np.where(flat == 0, np.nan, flat)

        if fillna:
            # forward-fill then backward-fill using numpy
            mask = np.isnan(flat)
            idx = np.where(~mask, np.arange(mask.shape[0])[:, None], 0)
            np.maximum.accumulate(idx, axis=0, out=idx)
            flat = flat[idx, np.arange(flat.shape[1])]

            mask = np.isnan(flat)
            idx = np.where(~mask, np.arange(mask.shape[0])[::-1][:, None], 0)
            np.maximum.accumulate(idx, axis=0, out=idx)
            flat = flat[-idx - 1, np.arange(flat.shape[1])]

        if how == "median":
            result = np.nanmedian(flat, axis=0)
        elif how == "min":
            result = np.nanmin(flat, axis=0)
        else:  # max
            result = np.nanmax(flat, axis=0)

        out[b] = result.reshape(band_data.shape[1], band_data.shape[2])

    return out