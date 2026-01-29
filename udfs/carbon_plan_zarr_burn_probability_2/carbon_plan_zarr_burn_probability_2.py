@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    min_val: float = 0.0,
    max_val: float = 0.15,
    mask_threshold: float = 0.001,
):
    import zarr
    import numpy as np

    common = fused.load("https://github.com/fusedio/udfs/tree/main/public/common/")

    zoom = common.estimate_zoom(bounds)

    pyramid_level = max(0, min(zoom - 1 + 4, 12))

    store = zarr.open(
        "https://carbonplan-share.s3.us-west-2.amazonaws.com/zarr-layer-examples/13-lvl-30m-4326-scott-BP.zarr",
        mode="r",
    )

    # Get the data array at the appropriate pyramid level
    data = store[pyramid_level]

    # Calculate pixel indices from bounds
    min_lon, min_lat, max_lon, max_lat = bounds

    # Get array shape and calculate pixel coordinates
    height, width = data.shape
    lon_res = 360 / width
    lat_res = 180 / height

    x_min = int((min_lon + 180) / lon_res)
    x_max = int((max_lon + 180) / lon_res)
    y_min = int((90 - max_lat) / lat_res)
    y_max = int((90 - min_lat) / lat_res)

    # Clamp to valid range
    x_min = max(0, min(x_min, width - 1))
    x_max = max(0, min(x_max, width))
    y_min = max(0, min(y_min, height - 1))
    y_max = max(0, min(y_max, height))

    # Read the data slice
    arr = data[y_min:y_max, x_min:x_max]

    if arr.size == 0:
        return None

    # Flip (lat ascending) - sortby equivalent
    arr = np.flipud(arr)

    # Mask and visualize (like the dust UDF)
    masked_data = np.nan_to_num(arr, nan=0)
    mask = arr > mask_threshold

    return common.visualize(
        data=masked_data,
        mask=mask,
        min=min_val,
        max=max_val,
        colormap="YlOrRd_r",
    )