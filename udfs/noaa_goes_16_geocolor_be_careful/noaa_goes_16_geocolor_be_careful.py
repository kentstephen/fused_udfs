@fused.udf
def udf(bounds: fused.types.Bounds=[-132.3, -40.9, 34.1, 75.1], time_slice: int = -1):
    import fsspec
    import xarray as xr
    import numpy as np
    from pyproj import Proj
    from datetime import datetime, timedelta
    
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    # convert bounds to tile
    tile = common.get_tiles(bounds, clip=True)
    # Get tile bounds
    tile_bounds = tile.total_bounds  # returns [minx, miny, maxx, maxy]
    minx, miny, maxx, maxy = tile_bounds
    
    def find_goes_file(fs, buckets, max_days=3, max_hours=12):
        """Search for available GOES files"""
        for bucket in buckets:
            print(f"Trying bucket: {bucket}")
            for days_back in range(max_days):
                target_time = datetime.utcnow() - timedelta(days=days_back)
                year = target_time.year
                day_of_year = target_time.timetuple().tm_yday
                
                for hours_back in range(max_hours):
                    hour = (target_time.hour - hours_back) % 24
                    day_adj = day_of_year - (1 if target_time.hour - hours_back < 0 else 0)
                    
                    path_pattern = f'{bucket}/ABI-L2-MCMIPC/{year}/{day_adj:03d}/{hour:02d}/'
                    
                    try:
                        files = fs.ls(path_pattern)
                        nc_files = [f for f in files if f.endswith('.nc')]
                        if nc_files:
                            return f's3://{nc_files[time_slice]}'
                    except:
                        continue
        
        return None
    
    # Open NOAA S3 bucket
    fs = fsspec.filesystem('s3', anon=True)
    
    # Try GOES-18 (West) first, then GOES-16
    buckets_to_try = ['noaa-goes18', 'noaa-goes16']
    
    file_path = find_goes_file(fs, buckets_to_try)
    
    if file_path is None:
        raise FileNotFoundError("Could not find any GOES MCMIPC files")
    
    print(f"Using file: {file_path}")
    
    # Open dataset
    ds = xr.open_dataset(file_path, engine='h5netcdf')
    
    # Get projection info from the dataset
    sat_h = ds['goes_imager_projection'].perspective_point_height
    sat_lon = ds['goes_imager_projection'].longitude_of_projection_origin
    sat_sweep = ds['goes_imager_projection'].sweep_angle_axis
    
    # Get x and y coordinates (in radians)
    x = ds['x'].data * sat_h
    y = ds['y'].data * sat_h
    
    # Create meshgrid
    xx, yy = np.meshgrid(x, y)
    
    # Set up projection
    p = Proj(proj='geos', h=sat_h, lon_0=sat_lon, sweep=sat_sweep)
    
    # Convert to lat/lon
    lons, lats = p(xx, yy, inverse=True)
    
    # Create mask from tile bounds
    mask = (lats >= miny) & (lats <= maxy) & (lons >= minx) & (lons <= maxx)
    
    # Extract RGB channels
    R = ds['CMI_C02'].data
    G = ds['CMI_C03'].data
    B = ds['CMI_C01'].data
    
    # Clip values to 0-1 range
    R = np.clip(R, 0, 1)
    G = np.clip(G, 0, 1)
    B = np.clip(B, 0, 1)
    
    # Apply gamma correction
    gamma = 2.2
    R = np.power(R, 1/gamma)
    G = np.power(G, 1/gamma)
    B = np.power(B, 1/gamma)
    
    # Calculate "true" green
    G_true = 0.45 * R + 0.1 * G + 0.45 * B
    G_true = np.clip(G_true, 0, 1)
    
    # Apply mask to each channel (set to 0 outside bounds instead of NaN)
    R_masked = np.where(mask, R, 0)
    G_masked = np.where(mask, G_true, 0)
    B_masked = np.where(mask, B, 0)
    
    # Stack as RGB array (bands, height, width) for rasterio format
    rgb_image = np.stack([
        (R_masked * 255).astype('uint8'),
        (G_masked * 255).astype('uint8'),
        (B_masked * 255).astype('uint8')
    ], axis=0)
    
    return rgb_image