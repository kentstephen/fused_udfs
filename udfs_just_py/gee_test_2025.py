@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    import ee

    fused.utils.common.ee_initialize(service_account_name='fused-nyt-gee@fused-nyt.iam.gserviceaccount.com',key_path="/mnt/cache/gee_key.json")
    geom = ee.Geometry.Rectangle(*bbox.total_bounds)
    ic = ee.FeatureCollection("projects/sat-io/open-datasets/facebook/gmv_grid")
    # print(ic)
    filtered = ic.filterBounds(geom)
    features = filtered.getInfo()['features']  # Get all features and properties
    print(features)
# OR
    # first_feature = ic.first().getInfo()  # Get first feature to examine structure
    # ds = xarray.open_dataset(
    #     ic,
    #     engine='ee',
    #     geometry=geom,
    #     scale=1/2**max(0,bbox.z[0]) 
    # )
    # ds=ds.max(dim='time') 