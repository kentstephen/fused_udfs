@fused.udf
def udf(
    bounds: fused.types.Bounds = [30.7801,29.7096,31.6747,30.3458],
    time_of_interest="2022-09-01/2022-09-30",
    red_band = "red",
    green_band = "green",
    blue_band = "blue",
    nir_band = "nir08",
    scale: int =0.01,
    collection="landsat-c2-l2",
    cloud_threshold=10,
):
    """Display NDVI based on Landsat & STAC"""
    import odc.stac
    import numpy as np
    import palettable
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo

    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    zoom = common.estimate_zoom(bounds)

    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    
    # Search for imagery within a specified bounding box and time period
    items = catalog.search(
        collections=[collection],
        bbox=bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": cloud_threshold}},
    ).item_collection()
    print(f"Returned {len(items)} Items")

    # Determine the pixel spacing for the zoom level
    pixel_spacing = int(5 * 2 ** (15 - zoom))
    print(f"{pixel_spacing = }")

    # Load imagery into an XArray dataset
    odc.stac.configure_s3_access(requester_pays=True)
# The function expects bands in this order:
# [0] = red
# [1] = green  
# [2] = blue
# [3] = red (again, for NDVI calculation)
# [4] = nir (for NDVI calculation)

    # So you need to load 4 unique bands but arrange them as 5:
    red_band = "red"
    green_band = "green"
    blue_band = "blue"
    nir_band = "nir08"
    
    # Load bands in the correct order
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=["blue", "green", "red", "nir08"],  # 4 bands, not 5!
        resolution=pixel_spacing,
        bbox=bounds,
    ).astype(float)
    
    # Stack as-is - don't duplicate red band
    arr_rgbi = np.stack([
        ds["blue"].values,
        ds["green"].values,
        ds["red"].values,
        ds["nir08"].values
    ], axis=0)
    
    # The function will use:
    # - arr_rgbi[-2] (red) and arr_rgbi[-1] (nir) for NDVI
    # - arr_rgbi[0,1,2,3] for output bands
    
    arr_green = get_greenest_pixel(arr_rgbi, how="median", fillna=True)
    arr_green = arr_green[:3,:,:]
    arr_scaled = arr_green * 1.0 * scale
    arr_scaled = np.clip(arr_scaled, 0, 255).astype("uint8")
    return arr_scaled
# def get_greenest_pixel(arr_rgbi, how="median", fillna=True):
def get_greenest_pixel(arr_rgbi, how="median", fillna=False):
    import numpy as np
    import pandas as pd

    # First 3 bands to visualize, last 2 bands to calculate NDVI
    out = (arr_rgbi[-1] * 1.0 - arr_rgbi[-2] * 1.0) / (
        arr_rgbi[-1] * 1.0 + arr_rgbi[-2] * 1.0
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

    out_flat = out_flat[median_index, np.arange(out.shape[1] * out.shape[2])]

    output_bands = []

    for b in [0, 1, 2, 3]:
        out_flat = arr_rgbi[b].reshape(t_len, out.shape[1] * out.shape[2])

        # Replace 0s with NaNs
        out_flat = np.where(out_flat == 0, np.nan, out_flat)
        if fillna:
            out_flat = pd.DataFrame(out_flat).ffill().bfill().values
        out_flat = out_flat[median_index, np.arange(out.shape[1] * out.shape[2])]
        output_bands.append(out_flat.reshape(out.shape[1], out.shape[2]))
    return np.stack(output_bands)