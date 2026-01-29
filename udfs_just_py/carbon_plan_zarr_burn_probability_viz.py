@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    min_val: float = 0.0,
    max_val: float = 0.15,
    chip_len: int = 512,
  mask_threshold: float= 0.0001        # Values <= this are transparent
):
    import zarr
    import numpy as np
    from scipy import ndimage
    import rioxarray as rio

    common = fused.load("https://github.com/fusedio/udfs/tree/main/public/common/")

    # Get zoom from bounds
    xmin, ymin, xmax, ymax = bounds
    zoom = common.estimate_zoom(bounds)

    # zarr-layer level selection:
    # mapPixelsPerWorld = 256 * 2^zoom
    # CONUS covers ~62° lon out of 360° = 0.172 worldFraction
    # Level 12 has 208881 lon pixels -> effective = 208881/0.172 ~ 1.2M pixels (zoom ~12)
    # Level 0 has 50 lon pixels -> effective ~ 290 pixels (zoom ~0)
    # Each level doubles resolution, so level ~ zoom - 1 (with clamping)

    pyramid_level = max(0, min(zoom - 1+4, 12))
    print(f"Zoom: {zoom}, Pyramid level: {pyramid_level}")

    store = zarr.open(
        "https://carbonplan-share.s3.us-west-2.amazonaws.com/zarr-layer-examples/13-lvl-30m-4326-scott-BP.zarr",
        mode='r'
    )
    level = store[str(pyramid_level)]

    lat = level['latitude'][:]
    lon = level['longitude'][:]

    # Check if bounds intersect data
    data_lon_min, data_lon_max = lon.min(), lon.max()
    data_lat_min, data_lat_max = lat.min(), lat.max()

    if xmax < data_lon_min or xmin > data_lon_max or ymax < data_lat_min or ymin > data_lat_max:
        print(f"Bounds outside data extent")
        return None

    # Clamp bounds to data extent
    xmin_clamped = max(xmin, data_lon_min)
    xmax_clamped = min(xmax, data_lon_max)
    ymin_clamped = max(ymin, data_lat_min)
    ymax_clamped = min(ymax, data_lat_max)

    # Find indices - lat is ascending in this dataset
    lon_idx = np.where((lon >= xmin_clamped) & (lon <= xmax_clamped))[0]
    lat_idx = np.where((lat >= ymin_clamped) & (lat <= ymax_clamped))[0]

    if len(lon_idx) == 0 or len(lat_idx) == 0:
        return None

    # Load data slice
    arr = level['BP'][lat_idx.min():lat_idx.max()+1, lon_idx.min():lon_idx.max()+1]
    arr = np.array(arr, dtype=np.float32)

    print(f"Data shape: {arr.shape}, range: [{np.nanmin(arr):.4f}, {np.nanmax(arr):.4f}]")

    if arr.size == 0:
        return None

    # Flip if needed (Fused expects north-up, zarr lat is ascending = south-up)
    arr = np.flipud(arr)

    # Create mask BEFORE resampling (values above threshold are visible)
    mask = (arr > mask_threshold).astype(np.float32)

    # Resample both data and mask
    if arr.shape[0] > 1 and arr.shape[1] > 1:
        zoom_factors = (chip_len / arr.shape[0], chip_len / arr.shape[1])
        arr = ndimage.zoom(arr, zoom_factors, order=1)
        mask = ndimage.zoom(mask, zoom_factors, order=0)  # nearest neighbor for mask
    # return arr
    # Get RGB from colormap (no opacity baked in)
    rgb = common.arr_to_plasma(
        arr,
        min_max=(min_val, max_val),
        colormap="YlOrRd_r",
        include_opacity=False,  # Just RGB, we'll add our own alpha
    )

    # Create binary alpha channel: 255 where valid data, 0 where masked
    # This gives full opacity for all visible burn probability values
    alpha = (mask > 0.5).astype(np.uint8) * 255
    
    # Stack RGB + Alpha into RGBA (4, H, W)
    rgba = np.concatenate([rgb, alpha[np.newaxis, :, :]], axis=0)

    return rgba
