import fused
@fused.udf
def udf(bbox=None):
    import geopandas as gpd
    import shapely
    import ee
    import xarray
    from utils import ee_initialize
    
    
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
    scale=1/2**max(0,bbox.z[0]) #increasing this will increase your resolution per z but slow down the loading
    print(scale)

    VARNAME='b1';
    # DATASET = "projects/sat-io/open-datasets/ORNL/LANDSCAN_GLOBAL"
    # DATASET = "projects/sat-io/open-datasets/global_freshwater_variables/open_water_avg"
    # DATASET = "projects/sat-io/open-datasets/WORLD-BANK/global-ext-heat-hazard"
    DATASET = "projects/sat-io/open-datasets/GLOBGM/TRANSIENT/WTD"
    DATASET = "projects/sat-io/open-datasets/GLOBGM/TRANSIENT/WTD-BOTTOM"
    
    min_max=(0,8000)
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
    arr = utils.arr_to_plasma(ds[VARNAME].values.squeeze().T,min_max=min_max)
    print(arr.sum())
    return arr
