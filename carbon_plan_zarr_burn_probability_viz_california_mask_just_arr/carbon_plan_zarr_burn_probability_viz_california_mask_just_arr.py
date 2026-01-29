@fused.udf
def udf(
    bounds: fused.types.Bounds = [-126.6, 20.9, -64.9, 50.4],
    min_val: float = 0.0,
    max_val: float = 0.15,
    chip_len: int = 512,
    mask_threshold: float = 0.0001
):
    import zarr
    import xarray as xr
    import numpy as np
    from scipy import ndimage
    from shapely.geometry import box
    
    common = fused.load("https://github.com/fusedio/udfs/tree/main/public/common/")
    
    # Get California boundary (cached)
    california = get_california_boundary()
    
    # Check if bounds intersect California
    bounds_geom = box(*bounds)
    if not california.geometry.iloc[0].intersects(bounds_geom):
        print("Bounds outside California")
        return None
    
    # Clip California to current tile bounds for masking
    california_clipped = california.clip(bounds)
    
    xmin, ymin, xmax, ymax = bounds
    zoom = common.estimate_zoom(bounds)
    
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
        return None
    
    arr = level['BP'][lat_idx.min():lat_idx.max()+1, lon_idx.min():lon_idx.max()+1]
    arr = np.array(arr, dtype=np.float32)
    print(f"Data shape: {arr.shape}, range: [{np.nanmin(arr):.4f}, {np.nanmax(arr):.4f}]")
    
    if arr.size == 0:
        return None
    
    arr = np.flipud(arr)
    
    # Create California geometry mask (True = outside geometry)
    geom_mask = common.gdf_to_mask_arr(california_clipped, arr.shape[-2:])
    
    # Apply mask: set values outside California to NaN
    arr = np.where(geom_mask, np.nan, arr)
    
    # Get actual coordinates for this slice                                                                                                
    lon_slice = lon[lon_idx.min():lon_idx.max()+1]                                                                                         
    lat_slice = lat[lat_idx.min():lat_idx.max()+1][::-1]  # flip to match arr                                                              
                                                                                                                                         
    # Return as xarray DataArray with coordinates                                                                                          
    da = xr.DataArray(                                                                                                                     
        arr,                                                                                                                               
        dims=['y', 'x'],                                                                                                                   
        coords={                                                                                                                           
            'y': lat_slice,                                                                                                                
            'x': lon_slice,                                                                                                                
        }                                                                                                                                  
    )                                                                                                                                      
    return da  


@fused.cache
def get_california_boundary():
    """Load and dissolve California census tracts into single boundary."""
    import geopandas as gpd
    
    print("Loading California tracts...")
    gdf = gpd.read_file(
        'https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/06_CALIFORNIA/06/tl_rd22_06_tract.zip'
    )
    
    california = gdf.dissolve()
    california.geometry = california.geometry.simplify(0.001)
    
    print(f"California boundary created")
    return california