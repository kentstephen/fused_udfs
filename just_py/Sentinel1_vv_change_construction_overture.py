@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    provider="AWS",
    time_of_interest="2024-01-01/2024-12-31",
    h3_size:int= 10
):  
    """
    This UDF returns Sentinel 2 NDVI of the passed bounding box (viewport if in Workbench, or {x}/{y}/{z} in HTTP endpoint)
    Data fetched from either AWS S3 or Microsoft Planterary Computer
    """
    import odc.stac
    import planetary_computer
    import pystac_client
    import shapely
    import geopandas as gpd
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
    # Example logic to scale resolution by latitude
    lat_range = bbox.total_bounds[1:3]  # ymin, ymax
    mid_lat = np.mean(lat_range)
    adjusted_res = 0.005 * np.cos(np.radians(mid_lat))
    if len(items) < 1:
        print(f'No items found. Please either zoom out or move to a different area')
    else:
        print(f"Returned {len(items)} Items")
    # After getting the items, let's print the available bands
    # print("Available bands:")
    # for item in items:
    #     print(item.assets.keys())
    # Load both VV and VH bands
    # ds = odc.stac.load(
    #     items,
    #     crs="EPSG:4326",
    #     bands=['vv', 'vh'],
    #     resolution=resolution,
    #     bbox=bbox.total_bounds,
    #     chunks={'time': 1},
    # ).astype(float)
    
    # # Get last date and calculate one year prior
    # last_date = ds.time.values[-1]
    # one_year_ago = last_date - np.timedelta64(365, 'D')
    
    # # Filter by date instead of slice
    # early_vv = ds.vv.where(ds.time <= one_year_ago).median(dim='time')
    # late_vv = ds.vv.where(ds.time > one_year_ago).median(dim='time')
    
#     # Calculate change
#     # change = late_vv - early_vv

    # Get orbit states for each item
    orbit_states = [item.properties['sat:orbit_state'] for item in items]
    
    # Get indices of ascending passes
    ascending_indices = [i for i, orbit in enumerate(orbit_states) if orbit == 'ascending']
    
    # Filter items to only ascending passes
    ascending_items = [items[i] for i in ascending_indices]

    print(adjusted_res)
    # For NYC area, use UTM Zone 18N (EPSG:32618)
    ds = odc.stac.load(
        ascending_items,
        crs="EPSG:4326",
        bands=['vv'],
        resolution=adjusted_res,  # Much smaller resolution
        bbox=bbox.total_bounds,
        chunks={'time': 1}
    )

# Filter for ascending only (or descending - pick one)
    # Print metadata from a few items to see where orbit info is stored
    # # Try printing the full item metadata
    # for item in items[:1]:  # Just look at first item
    #     print("Properties:")
    #     print(item.properties)
    
    # Filter with appropriate range for linear values
    # ds_filtered = ds.where((ds.vv > 100) & (ds.vv < 3000))
    # For AWS data
    # Check raw values first
    print("Raw data stats:")
    print(ds.vv.values.min(), ds.vv.values.max())
    
    # If applying log transform, add small offset to avoid -inf
    if provider == "AWS":
        ds['vv'] = ds.vv.where(ds.vv > 0)  # Mask zeros/negatives
    # Or add small offset
    # ds['vv'] = 10 * np.log10(ds.vv + 1e-10)
    
    # For MSPC data
    
    # Apply temporal smoothing
    # ds_smooth = ds_filtered.rolling(time=3, center=True).median()
    
    # Calculate change
    # Look at maximum change between consecutive timestamps
    # If you're just defining arr once:
    consecutive_diff = ds.vv.diff(dim='time')
    max_change = consecutive_diff.max(dim='time')
    arr = max_change.values
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
    # print("Raw data stats:")
    # print(f"Total points: {len(df_vv)}")
    # print(f"Lat range: {df_vv['lat'].min()} to {df_vv['lat'].max()}")
    # print(f"Lng range: {df_vv['lng'].min()} to {df_vv['lng'].max()}")
    # print(f"Data range: {df_vv['data'].min()} to {df_vv['data'].max()}")
    #     # boundary = get_usa_boundary()
    print(df_vv['data'].describe())
    print(df_vv)
    # print(boundary)
    def df_to_hex(df_vv,bounds, h3_size):
        
        
        xmin, ymin, xmax, ymax = bounds
        con = fused.utils.common.duckdb_connect()
        qr = f"""
with to_grid as(
SELECT 
    h3_latlng_to_cell(lat::double, lng::double, {h3_size}::integer) AS h3,
    unnest(h3_grid_disk(h3, 3)) as hex,
    data
    
  
FROM df_vv

--GROUP BY hex
--having avg(data) >300
)
select  hex,
h3_cell_to_boundary_wkt(hex) as boundary,
  h3_cell_to_lat(hex) cell_lat,
    h3_cell_to_lng(hex) cell_lng,
        avg(data) as data
        from to_grid
        WHERE (cell_lat >= {ymin}
    AND cell_lat < {ymax}
    AND cell_lng >= {xmin}
    AND cell_lng < {xmax})
        group by hex

        """
        df = con.sql(qr).df()
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    
    gdf = df_to_hex(df_vv,bounds, h3_size)
    
    print(gdf['data'].describe())
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, use_columns=['geometry', 'height'], min_zoom=10)
    
    # Join H3 with Buildings using coffe_score to visualize, you can change to a left join
    gdf_joined = gdf_overture.sjoin(gdf, how="inner", predicate="intersects")
    gdf_joined = gdf_joined.drop(columns='index_right')
    return gdf_joined
