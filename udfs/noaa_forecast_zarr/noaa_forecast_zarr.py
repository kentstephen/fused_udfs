@fused.udf
def udf(
    bounds: fused.types.Bounds = [-180, -90, 180, 90],
    layer: str = 'total_cloud_cover_atmosphere',
    min_max = (0, 100),
    hours_ago: int = 0,
    lead_time_hours: int = 0
):
    """UDF to visualize cloud cover from the NOAA HRRR dataset."""
    
    import numpy as np
    import icechunk
    import zarr
    from datetime import datetime, timedelta
    
    # Load utils
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")
    
    store = zarr.storage.FsspecStore.from_url(
        's3://us-west-2.opendata.source.coop/dynamical/noaa-hrrr-forecast-48-hour/v0.1.0.zarr',
        read_only=True,
        storage_options={'anon': True}
    )
    
    # Open Zarr group
    z = zarr.open_group(store=store, mode='r')
    
    # Get coordinate arrays
    lat = z["latitude"][:]
    lon = z["longitude"][:]
    init_times = z["init_time"][:]  # Unix timestamps (integers)
    lead_times = z["lead_time"][:]  # timedelta64 array
    
    # Get current time and calculate target
    now = datetime.utcnow()
    target_time = now - timedelta(hours=hours_ago)
    target_time = target_time.replace(minute=0, second=0, microsecond=0)
    target_timestamp = int(target_time.timestamp())
    
    print("Target time:", target_time)
    print("Bounds:", bounds)
    
    # Get bounds
    minx, miny, maxx, maxy = bounds
    
    # 1. Find matching init_time index (closest available)
    time_diffs = np.abs(init_times - target_timestamp)
    init_idx = np.argmin(time_diffs)
    
    # 2. Find matching lead_time index
    lead_time_td = np.timedelta64(lead_time_hours, 'h')
    lead_time_match = np.where(lead_times == lead_time_td)[0]
    if len(lead_time_match) == 0:
        lead_time_idx = 0
    else:
        lead_time_idx = lead_time_match[0]
    
    # 3. Get index ranges for lat/lon slices
    lat_mask = (lat >= float(miny)) & (lat <= float(maxy))
    lon_mask = (lon >= float(minx)) & (lon <= float(maxx))
    
    lat_idx = np.where(lat_mask)[0]
    lon_idx = np.where(lon_mask)[0]
    
    if len(lat_idx) == 0 or len(lon_idx) == 0:
        return None
    
    # 4. Select data from the zarr array directly
    var_arr = z[layer][init_idx, lead_time_idx, lat_idx.min():lat_idx.max()+1, lon_idx.min():lon_idx.max()+1]
    
    # No air density computation needed for cloud cover
    da = var_arr
    print(da)
    
    # Cloud color scheme
    bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, np.inf]
    colors = [
        "#87CEEB", "#B0E0E6", "#E0F6FF", "#F0F8FF", "#FFFFFF",
        "#F5F5F5", "#E8E8E8", "#D3D3D3", "#B0B0B0", "#808080", "#606060"
    ]
    
    data_array = np.flipud(da)  # flip vertically
    
    # Mask and visualize
    masked_data = np.nan_to_num(data_array, nan=0)
    mask = data_array >= bins[1]
    
    viz = common_utils.visualize(
        data=masked_data,
        mask=mask,
        min=min_max[0],
        max=min_max[1],
        colormap=colors,
        colorbins=bins
    )
    
    return viz