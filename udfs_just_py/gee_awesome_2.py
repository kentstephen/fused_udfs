import fused
@fused.udf
def udf(bbox=None):
    import geopandas as gpd
    import shapely
    import ee
    import xarray
    import numpy as np
    from utils import ee_initialize, arr_to_cmap
    # Define your hex color palette
    hex_palette = ['ffffff', 'ffff00', 'ffcc00', 'ff9900', 'ff0000', '990000']

# Normalize the hex colors to RGB values between 0 and 1
    color_palette = [tuple(int(hex_color[i:i+2], 16) / 255. for i in (0, 2, 4)) for hex_color in hex_palette]
    
    env_path = '/mnt/cache/gee_key.json'
    # Helper function to authenticate with local key.json
    ee_initialize(
        service_account_name='fused-nyt-gee@fused-nyt.iam.gserviceaccount.com',
        key_path=env_path
    )

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils
    
    geom = ee.Geometry.Rectangle(*bbox.total_bounds)
    scale=1/2**max(0,bbox.z[0]) 
    
  # Using a smaller base #increasing this will increase your resolution per z but slow down the loading
    print(scale)

    VARNAME='b1';
    # DATASET = "projects/sat-io/open-datasets/ORNL/LANDSCAN_GLOBAL"
    # DATASET = "projects/sat-io/open-datasets/global_freshwater_variables/open_water_avg"
    # DATASET = "projects/sat-io/open-datasets/open-aerial-map"
    # DATASET = "projects/sat-io/open-datasets/global_wind_farms_2020"
    # DATASET = "projects/sat-io/open-datasets/GLOBGM/TRANSIENT/WTD"
    # DATASET = "projects/sat-io/open-datasets/GLOBGM/TRANSIENT/WTD-BOTTOM"
    # DATASET = "projects/wri-datalab/cities/urban_land_use/V1"
    DATASET = "projects/sat-io/open-datasets/ORNL/LANDSCAN_GLOBAL"
    min_max=(0,185000)
    ic = ee.ImageCollection(DATASET)#.filter(ee.Filter.date('2023-01-01', '2023-06-01')))
    # ic = ee.Image(DATASET)#.filter(ee.Filter.date('2023-01-01', '2023-06-01')))


    ds = xarray.open_dataset(
        ic,
        engine='ee',
        projection='3857',
        geometry=geom,
        scale=scale
    ).isel(time=0)   
    min_max = (ds[VARNAME].values.min(), ds[VARNAME].values.max()*10)
    print(min_max)
    print(ds)
    print(list(ds.variables))
    arr = arr_to_cmap(ds[VARNAME].values.squeeze().T, color_palette, norm_range=min_max)


    # arr = ds[VARNAME].values.squeeze().T
    print(arr.sum())
    return arr
