# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
    bounds: fused.types.Bounds=None,
    collection="alos-dem",
    band="data",
    res_factor:int=9,
    res:int=13
):
    import stackstac
    import odc.stac
    import rasterio
    import palettable
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    from utils import aggregate_df_hex, df_to_hex
    import numpy as np
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    items = catalog.search( 
        collections=[collection],
        bbox=bounds,
    ).item_collection()
    if not items or len(items) == 0:
        print("No items found for the given bounds and collection")
        return
    # print(items[0].assets.keys()) 
    # print(f"Returned {len(items)} Items")
    resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    if len(items) <= 0:
        return
        # item = items[0]
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[band],
        resolution=resolution,
        bbox=bounds,
    ).astype(float)
    
    # Use data from the most recent time.
    arr = ds[band].max(dim="time")
        # Mask no-data values (typically 0 or negative)
        # arr = np.where(dsm > 0, dsm, np.nan)
        # Basic array statistics and information
    print(f"Array shape: {arr.shape}")
    print(f"Data type: {arr.dtype}")
    print(f"Contains NaN values: {np.isnan(arr).any()}")
    print(f"Percentage of NaN values: {np.isnan(arr).sum() / arr.size * 100:.2f}%")

    # Value distribution
    # non_nan_values = arr[~np.isnan(arr)]
    # print(f"Non-NaN count: {len(non_nan_values)}")
    # if len(non_nan_values) > 0:
    #     print(f"Min value: {np.min(non_nan_values)}")
    #     print(f"Max value: {np.max(non_nan_values)}")
    #     print(f"Mean value: {np.mean(non_nan_values)}")
    #     print(f"Median value: {np.median(non_nan_values)}")
        
    #     # Show a small sample of the actual values
    #     print("Sample of values:")
    #     sample_indices = np.where(~np.isnan(arr))
    #     sample_size = min(10, len(sample_indices[0]))
    #     for i in range(sample_size):
    #         row = sample_indices[0][i]
    #         col = sample_indices[1][i]
    #         print(f"  Value at [{row}, {col}]: {arr[row, col]}")
        
    # # Print a slice of the array where data exists
    # valid_rows = np.where(~np.isnan(arr).any(axis=1))[0]
    # if len(valid_rows) > 0:
    #     mid_row = valid_rows[len(valid_rows)//2]
    #     print(f"Row {mid_row} slice (10 values):")
    #     row_data = arr[mid_row]
    #     non_nan_indices = np.where(~np.isnan(row_data))[0]
    #     if len(non_nan_indices) > 0:
    #         start_idx = non_nan_indices[0]
    #         print(row_data[start_idx:start_idx+10])
    #     print(arr)
    #     # Use data from the most recent time.
    #     arr = np.where(dsm > 0, dsm, np.nan)
    if arr is None:
        return
    # return arr
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
        # bounds = utils.bounds_to_gdf(bounds)
        # bounds_values = bounds.bounds.values[0]
    df = utils.arr_to_latlng(arr.values, bounds)
    bounds_gdf = utils.bounds_to_gdf(bounds)
    bounds_values = bounds_gdf.bounds.values[0]
    df = aggregate_df_hex(df=df,stats_type='mean', res=res, bounds_values=bounds_values)
    df["elev_scale"] = int((15 - zoom) * 1)

    # df['metric'] = df["metric"] - 2000
    print(df)
    return df

    # rgb_image = utils.visualize(
    #     data=arr,
    #     min=0,
    #     max=100,
    #     colormap=palettable.matplotlib.Viridis_20,
    # )
    # return rgb_image