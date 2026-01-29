@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    provider="MSPC",
    time_of_interest="2024-01-01/2024-12-30",
    h3_size:int= 8
):  
    """
    This UDF returns Sentinel 2 NDVI of the passed bounding box (viewport if in Workbench, or {x}/{y}/{z} in HTTP endpoint)
    Data fetched from either AWS S3 or Microsoft Planterary Computer
    """
    import odc.stac
    import planetary_computer
    import pystac_client
    import shapely
    from pystac.extensions.eo import EOExtension as eo
    import utils
    import numpy as np
    from utils import get_usa_boundary
    if provider == "AWS":
        red_band = "red"
        nir_band = "nir"
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif provider == "MSPC":
        red_band = "B04"
        nir_band = "B08"
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
    else:
        raise ValueError(
            f'{provider=} does not exist. provider options are "AWS" and "MSPC"'
        )
    
    items = catalog.search(
            collections=["sentinel-1-grd"],
            bbox=bbox.total_bounds,
            datetime=time_of_interest,
            query=None,
        ).item_collection()
    
    # Capping resolution to min 10m, the native Sentinel 2 pixel size
    resolution = min(0.005, 10 * 2 ** max(0, (14 - bbox.z[0])))
    print(f"{resolution=}")

    if len(items) < 1:
        print(f'No items found. Please either zoom out or move to a different area')
    else:
        print(f"Returned {len(items)} Items")
    
    ds = odc.stac.load(
        items,
        crs="EPSG:4326",
        bands=['vv'],
        resolution=resolution,
        bbox=bbox.total_bounds,
        chunks={'time': -1},
    ).astype(float)
    # After loading the dataset, split it into early and late periods
    early_period = ds.vv.isel(time=slice(None, len(ds.time)//2)).mean(dim='time')
    late_period = ds.vv.isel(time=slice(len(ds.time)//2, None)).mean(dim='time')
    
    # Calculate change and convert to dB
    change = late_period - early_period
    change = 10 * np.log10(np.abs(change + 1e-10))
    
    # Use this array instead of the max
    arr = change.values
#     gdf_w_bbox = get_usa_boundary(bbox)
# # create a geom mask
#     geom_mask = fused.utils.common.gdf_to_mask_arr(gdf_w_bbox, arr.shape, first_n=1)
    
#     # Check array and mask shapes
#     print("Array shape:", arr.shape)
#     print("Mask shape:", geom_mask.shape)
    
#     # Since arr appears to be 2D, we don't need to replicate the mask
#     arr = np.ma.masked_array(arr, mask=geom_mask)
    # You might want to adjust these values based on your data
    # print("Change statistics:")
    # print("Min change:", np.nanmin(arr))
    # print("Max change:", np.nanmax(arr))
    # print("Mean change:", np.nanmean(arr))
        
    # # Convert to dB
    # change = 10 * np.log10(np.abs(change + 1e-10))
    
    # arr = ds['vv'].max(dim='time').values # highlight high backscatter areas like buildings
    # # print(ds)
    # print("Min value:", ds.vv.min().values)
    # print("Max value:", ds.vv.max().values)
    # rgb_image = utils.visualize(
    #     data=arr,
    #     min=-56,
    #     max=2000,
    #     colormap=['black', 'green']
    # )
    bounds = bbox.bounds.values[0]
    arr_to_latlng = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_latlng
    # # Debug the dataset
    # print("Dataset info:", ds.info())
    # print("VV band stats:")
    # print("Min raw value:", ds.vv.min().values)
    # print("Max raw value:", ds.vv.max().values)
    # print("Number of NaN values:", np.isnan(ds.vv).sum().values)
    # arr = ds.vv.max(dim="time")# highlight high backscatter areas like buildings
    # Skip log transformation if data is already in dB range
    # if np.nanmedian(arr) < 0:  # Raw backscatter values are typically very small (<<1)
    #     arr = 10 * np.log10(np.abs(arr + 1e-10))
    # else:
    #     print("Data appears to already be in dB units, skipping log transformation")
    # if np.nanmedian(arr) < 0:
    #     arr = 10 * np.log10(np.abs(arr + 1e-10))
    # else:
    # # If already in dB, consider normalizing
    #     arr = (arr - arr.mean()) / arr.std()  # Z-score normalization
    # arr = 10 * np.log10(arr)
    # arr = (arr - arr.mean()) / arr.std() 
    # print(arr)
    # Debug the final array
    # print("Final array stats:")
    # print("Shape:", arr.shape)
    # print("Min value:", arr.min().values)
    # print("Max value:", arr.max().values)
    # print("Number of NaN values:", np.isnan(arr).sum().values)
    df_vv = arr_to_latlng(arr, bounds)
    # boundary = get_usa_boundary()
    print(df_vv['data'].describe())
    print(df_vv)
    # print(boundary)
    def df_to_hex(df_vv,bounds, h3_size):
        
        
        xmin, ymin, xmax, ymax = bounds
        con = fused.utils.common.duckdb_connect()
        qr = f"""
       SELECT 
    h3_latlng_to_cell(lat::double, lng::double, {h3_size}::integer) AS hex,
    h3_cell_to_lat(hex) cell_lat,
    h3_cell_to_lng(hex) cell_lng,
    sum(data) as data
FROM df_vv
WHERE cell_lat >= {ymin}
    AND cell_lat < {ymax}
    AND cell_lng >= {xmin}
    AND cell_lng < {xmax}
  
GROUP BY hex
        """
        return con.query(qr).df()
    
    df = df_to_hex(df_vv,bounds, h3_size)
    print(df['data'].describe())
    return df
