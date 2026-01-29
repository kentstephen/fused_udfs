import json
import fused  # make fused available for decorators and calls

# ----------------------------------------------------------------------
# Parameter definitions (JSON strings) for the supported collections
# ----------------------------------------------------------------------
# MODIS
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

# LANDSAT
landsat_params = json.dumps(
    {
        "collection": "landsat-c2-l2",
        "band_list": ["blue", "green", "red", "nir08"],
        "time_of_interest": "2016-09-01/2016-12-30",
        "query": {"eo:cloud_cover": {"lt": 5}},
        "scale": 0.01,
    }
)

# SENTINEL-2
sentinel_params = json.dumps(
    {
        "collection": "sentinel-2-l2a",
        "band_list": ["B02", "B03", "B04", "B08"],
        "time_of_interest": "2021-09-01/2021-12-30",
        "query": {"eo:cloud_cover": {"lt": 5}},
        "scale": 0.1,
    }
)

# ----------------------------------------------------------------------
# Global helper: cache the loading of the common utilities (ignores path warnings)
# ----------------------------------------------------------------------
@fused.cache
def get_common():
    # Load the common utilities without the deprecated 'path' argument
    return fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

@fused.udf
def udf(
    bounds: fused.types.Bounds = [-122.463, 37.755, -122.376, 37.803],
    collection_params=sentinel_params,
    chip_len: int = 512,
    how: str = "max",  # median, min. default is max
    fillna: bool = False,  # This addresses stripes
):
    import numpy as np
    import pandas as pd

    # Load the cached common utilities
    common = get_common()

    # Convert bounds to tile (clipped to the viewport)
    tile = common.get_tiles(bounds, clip=True)

    # Unpack the JSON collection parameters
    collection, band_list, time_of_interest, query, scale = json.loads(
        collection_params
    ).values()

    print(collection, band_list, time_of_interest, query)

    # Search the Planetary Computer catalog (keep it lightweight)
    stac_items = search_pc_catalog(
        bounds=tile,
        time_of_interest=time_of_interest,
        query=query,
        collection=collection,
        max_items=2,  # reduced to minimise server load
    )
    if not stac_items:
        return

    print("Processing stac_items:", len(stac_items))
    print(stac_items[0].assets.keys())
    df_tiff_catalog = create_tiffs_catalog(stac_items, band_list)

    # Read all required TIFFs in parallel
    arrs_out = run_pool_tiffs(tile, df_tiff_catalog, output_shape=(chip_len, chip_len))

    # Guard against an empty result from the pool
    if arrs_out.size == 0:
        print("No TIFF data could be read")
        return

    # Generate an array with the greenest pixel (or median/min/max as requested)
    arr = get_greenest_pixel(arrs_out, how=how, fillna=fillna)[:3, :, :]

    # Scale to 8-bit values
    arr_scaled = arr * scale
    arr_scaled = np.clip(arr_scaled, 0, 255).astype("uint8")
    return arr_scaled, bounds
    # TODO: color correction https://custom-scripts.sentinel-hub.com/custom-scripts/sentinel-2/poor_mans_atcor/


# ----------------------------------------------------------------------
# Helper: run a pool of TIFF reads (uses the global cached common utilities)
# ----------------------------------------------------------------------
def run_pool_tiffs(bounds, df_tiffs, output_shape):
    import numpy as np

    columns = df_tiffs.columns

    @fused.cache
    def fn_read_tiff(tiff_url, bounds=bounds, output_shape=output_shape):
        try:
            common = get_common()
            return common.read_tiff(bounds, tiff_url, output_shape=output_shape)
        except Exception as e:
            # Log the error and return None so the pool can continue
            print(f"Failed to read TIFF {tiff_url}: {e}")
            return None

    # Build a flat list of all TIFF URLs we need to read
    tiff_list = [
        df_tiffs[band].iloc[i] for band in columns for i in range(len(df_tiffs))
    ]

    common = get_common()
    arrs_tmp = common.run_pool(fn_read_tiff, tiff_list)

    # Filter out any None entries caused by read failures
    arrs_tmp = [a for a in arrs_tmp if a is not None]
    if not arrs_tmp:
        return np.empty(0)

    arrs_out = np.stack(arrs_tmp)
    arrs_out = arrs_out.reshape(
        len(columns), len(df_tiffs), output_shape[-2], output_shape[-1]
    )
    return arrs_out


def search_pc_catalog(
    bounds,
    time_of_interest,
    query={"eo:cloud_cover": {"lt": 5}},
    collection="sentinel-2-l2a",
    max_items: int = 20,
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
        max_items=max_items,
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


def get_greenest_pixel(arr_rgbi, how="median", fillna=True):
    import numpy as np
    import pandas as pd

    # NDVI calculation (using the last two bands)
    out = (arr_rgbi[-1] - arr_rgbi[-2]) / (arr_rgbi[-1] + arr_rgbi[-2])
    t_len = out.shape[0]
    out_flat = out.reshape(out.shape[0], -1)

    # Determine the pixel index based on the requested method
    if how == "median":
        median_index = np.argsort(out_flat, axis=0)[t_len // 2]
    elif how == "min":
        median_index = np.argmin(out_flat, axis=0)
    else:  # "max"
        median_index = np.argmax(out_flat, axis=0)

    output_bands = []
    for b in range(4):
        band_flat = arr_rgbi[b].reshape(t_len, -1)
        band_flat = np.where(band_flat == 0, np.nan, band_flat)
        if fillna:
            band_flat = pd.DataFrame(band_flat).ffill().bfill().values
        band_flat = band_flat[median_index, np.arange(out.shape[1] * out.shape[2])]
        output_bands.append(band_flat.reshape(out.shape[1], out.shape[2]))

    return np.stack(output_bands)