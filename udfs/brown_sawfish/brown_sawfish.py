# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, band: str="z", res: float = 9):
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/1469dfc/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    import fsspec
    import xarray as xr
    # import hvplot.xarray
    from utils import aggregate_df_hex
    fs = fsspec.filesystem('s3', anon=True, endpoint_url='https://data.source.coop') 
    fs.ls('rsignell/ncei-estuarine-bathymetry')
    
    ds = xr.open_dataset(fs.open('rsignell/ncei-estuarine-bathymetry/Chesapeake_Bay/chesapeake_bay_M130_2017.nc'))
    print(ds)
    # # Check all dimensions and coordinates
    # print(ds.dims)
    # print(ds.coords)
    
    # # Look for any time-related variables
    # print([var for var in ds.data_vars if 'time' in var.lower()])
    # print([coord for coord in ds.coords if 'time' in coord.lower()])
    
    # # Check the full structure
    # print(ds.info())
    # ds['Band1'].hvplot(x='x', y='y', crs='EPSG:26919', rasterize=True, tiles='OSM', cmap='viridis')
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    bounds = utils.bounds_to_gdf(bounds)
    bounds_values = bounds.bounds.values[0]
    arr = ds[band]
    df = utils.arr_to_latlng(arr.values, bounds)
    print(df)
    bounds = utils.bounds_to_gdf(bounds) 
    bounds = bounds.bounds.values[0]
    df = aggregate_df_hex(bounds,df=df,stats_type='mean', res=res)

    # print(df)
    print(df['metric'].describe())
    return df
