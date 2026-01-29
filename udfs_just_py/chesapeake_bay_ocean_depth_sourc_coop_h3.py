# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf#(cache_max_age="0s")
def udf(path: str='s3://fused-users/stephenkentdata/stephenkentdata/ocean_depth_2025/chesapeake_bay_M130_2017.nc',
         band: str = 'band',
        bounds: fused.types.Bounds = None, res: int = 10):
    # Using common fused functions as helper
    from utils import aggregate_df_hex
    import numpy as np
    import duckdb
    import pandas as pd
    common = fused.load("https://github.com/fusedio/udfs/tree/1469dfc/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    # common_utils = fused.load("https://github.com/fusedio/udfs/tree/2f41ae1/public/common/").utils
    tile = common.get_tiles(bounds, clip=True)
    import rioxarray as rxr
    @fused.cache#(cache_reset=True)
    def get_dem(tile, path):
        try:
            dem = rxr.open_rasterio(path)
            print(f"DEM CRS: {dem.rio.crs}")
            print(f"Tile CRS: {tile.crs}")
            
            # Store the original CRS before clipping
            original_crs = dem.rio.crs
            
            total_bounds = tile.total_bounds
            dem = dem.rio.clip_box(*total_bounds)
            
            # Always restore the CRS after clipping
            dem = dem.rio.write_crs(original_crs)
            print(f"DEM CRS after clipping: {dem.rio.crs}")
            return dem
            
        except Exception as e:
            print(f"Error in get_dem: {e}")
            return None
            
    dem = get_dem(tile, path)
    # print(f"DEM CRS after clipping: {dem.rio.crs}")
    if dem is None:
        return
    # dem = dem.rio.reproject(4326)
    # dem = dem.where(dem.notnull()) 
    print(f"DEM CRS after processing: {dem.rio.crs}")
    # dem = dem.where(dem > dem.rio.nodata, drop=False).rio.write_nodata(np.nan)
    # arr = dem[band]
    lons, lats = np.meshgrid(dem.x.values, dem.y.values)
    df = pd.DataFrame({
        'lng': lons.flatten(),
        'lat': lats.flatten(), 
        'data': dem.values.flatten()
    })
    df = df.dropna() 
    # df = common.arr_to_latlng(dem.values, bounds)
    df = df.dropna() 
    print(df)
    bounds = common.bounds_to_gdf(bounds) 
    bounds = bounds.bounds.values[0]
    df = aggregate_df_hex(bounds,df=df,stats_type='mean', res=res)
    df = duckdb.sql("from df where metric is not null").df()
    # df['metric'] = -df['metric'] + 150
    print(df)
    print(df['metric'].describe())
    return df
    