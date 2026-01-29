@fused.udf
def udf(
    bounds: fused.types.Bounds = [-126.6, 20.9, -64.9, 50.4],
    min_val: float = 0.01,
    max_val: float = 0.13,
    chip_len: int = 512,
    mask_threshold: float = 0.001,
    return_viz: bool = True
):
    import zarr
    import numpy as np
    from scipy import ndimage
    import xarray as xr
    import rioxarray

    common = fused.load("https://github.com/fusedio/udfs/tree/main/public/common/")

    # Bounds are in 4326 (matching zarr data)
    xmin, ymin, xmax, ymax = bounds
    zoom = common.estimate_zoom(bounds)
    print(f"Bounds: {bounds}")

    pyramid_level = max(0, min(zoom - 1 + 4, 12))
    print(f"Zoom: {zoom}, Pyramid level: {pyramid_level}")

    store = zarr.open(
        "https://carbonplan-share.s3.us-west-2.amazonaws.com/zarr-layer-examples/13-lvl-30m-4326-scott-BP.zarr",
        mode='r'
    )
    level = store[str(pyramid_level)]
    lat = level['latitude'][:]
    lon = level['longitude'][:]

    data_lon_min, data_lon_max = lon.min(), lon.max()
    data_lat_min, data_lat_max = lat.min(), lat.max()
    print(f"Data extent: lon [{data_lon_min}, {data_lon_max}], lat [{data_lat_min}, {data_lat_max}]")

    if xmax < data_lon_min or xmin > data_lon_max or ymax < data_lat_min or ymin > data_lat_max:
        print("Bounds outside data extent")
        return None

    xmin_clamped = max(xmin, data_lon_min)
    xmax_clamped = min(xmax, data_lon_max)
    ymin_clamped = max(ymin, data_lat_min)
    ymax_clamped = min(ymax, data_lat_max)

    lon_idx = np.where((lon >= xmin_clamped) & (lon <= xmax_clamped))[0]
    lat_idx = np.where((lat >= ymin_clamped) & (lat <= ymax_clamped))[0]

    if len(lon_idx) == 0 or len(lat_idx) == 0:
        print("No data in bounds")
        return None

    arr = level['BP'][lat_idx.min():lat_idx.max()+1, lon_idx.min():lon_idx.max()+1]
    arr = np.array(arr, dtype=np.float32)
    print(f"Data shape: {arr.shape}, range: [{np.nanmin(arr):.4f}, {np.nanmax(arr):.4f}]")

    if arr.size == 0:
        print("Empty array")
        return None

    arr = np.flipud(arr)

    # Get actual coordinates for this slice
    lon_slice = lon[lon_idx.min():lon_idx.max()+1]
    lat_slice = lat[lat_idx.min():lat_idx.max()+1][::-1]  # flip to match arr

    # Data mask (valid data)
    data_mask = (arr > mask_threshold) & ~np.isnan(arr)

    # Resample using ndimage.zoom (stays in 4326, no reprojection)
    if arr.shape[0] > 1 and arr.shape[1] > 1:
        zoom_factors = (chip_len / arr.shape[0], chip_len / arr.shape[1])
        arr = ndimage.zoom(arr, zoom_factors, order=1)
        data_mask = ndimage.zoom(data_mask.astype(np.float32), zoom_factors, order=0) > 0.5
    print(f"After zoom shape: {arr.shape}")

    # Replace NaN with 0
    arr = np.nan_to_num(arr, nan=0)

    # Return processed DataArray if viz not requested
    if not return_viz:
        da_out = xr.DataArray(
            arr,
            dims=['y', 'x'],
            coords={
                'y': np.linspace(ymax_clamped, ymin_clamped, arr.shape[0]),
                'x': np.linspace(xmin_clamped, xmax_clamped, arr.shape[1])
            }
        )
        da_out = da_out.rio.write_crs("EPSG:4326")
        return da_out

    rgb = common.arr_to_plasma(
        arr,
        min_max=(min_val, max_val),
        colormap="YlOrRd_r",
        include_opacity=False,
    )

    alpha = data_mask.astype(np.uint8) * 160

    rgba = np.concatenate([rgb, alpha[np.newaxis, :, :]], axis=0)
    print(f"Returning RGBA shape: {rgba.shape}")
    return rgba
