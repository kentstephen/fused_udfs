# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds=None, 
        time_of_interest="2024-02-20/2024-03-25", 
        chip_len:int=256, 
        scale:float=0.064,
       h3_size: int = 12):
    import geopandas as gpd
    import shapely
    import pandas as pd
    import numpy as np
    import duckdb
    from utils import get_arr, tile_to_df, df_to_hex

    # Using common fused functions as helper
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/482f6de/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds)
    # zoom =14

    print(zoom)
    
    # # # find the tiles with intersecting geom
    # gdf = gpd.read_file('s3://fused-users/stephenkentdata/2025/boundary_file_for_rocky_mountain/ROMO_BND_Boundary_py_NAD83_1_8499885102402078886.geojson')
    # gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(10_000).to_crs('EPSG:4326')
    # gdf_clipped = gdf.dissolve().to_crs(4326).clip(tile)
    # gdf_w_bounds = pd.concat([gdf_clipped,tile])
    # if len(gdf_w_bounds)<=1:
    #     print('No bounds is intersecting with the given geometry.')
    #     return 
        
    # read sentinel data
    # udf_sentinel = fused.load('https://github.com/fusedio/udfs/tree/a1c01c6/public/DC_AOI_Example/')
    arr = get_arr(tile, time_of_interest=time_of_interest, output_shape=(chip_len, chip_len), max_items=200)
    # print(arr.shape)
    if arr is None:
        return
    # arr = np.clip(arr *  scale, 0, 255).astype("uint8")[:3]
    arr = np.clip(arr * scale, 0, 255).astype("uint8")
    print(arr)
    
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # create a geom mask
    # geom_mask = utils.gdf_to_mask_arr(gdf_w_bounds, arr.shape[-2:], first_n=1)    
    # arr = np.ma.masked_array(arr, [geom_mask]*arr.shape[0])
    
    # convert arr to xyz dataframe
    df = tile_to_df(tile, arr)
    print(df)
    # h3_size = min(int(3+zoom/1.5),15)
    print(h3_size) 
    data_cols = [f'band{i+1}' for i in range(len(arr))]
    bounds_gdf = common_utils.bounds_to_gdf(bounds)
    bounds_values = bounds_gdf.bounds.values[0]
    df = df_to_hex(bounds_values, df, data_cols=data_cols, h3_size=h3_size, hex_col='hex', return_avg_lalng=False)
    print(df)

    # calculate stats: mean pixel value for each hex
    mask=1
    for col in data_cols:
        df[f'agg_{col}']=df[f'agg_{col}'].map(lambda x:x.mean())
        mask=mask*df[f'agg_{col}']>0
    df = df[mask]
    # print(zoom)
    # convert the h3_int to h3_hex
    # df['hex'] = df['hex'].map(lambda x:hex(x)[2:])
    # df_dem = fused.run("fsh_3M3RyItkeAZpGR6fMZ482r", bounds=bounds, res=h3_size)   # jaxa
    # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=bounds, h3_size=h3_size) # hexify
    # print(df_dem['hex'])
    # df_dem = fused.run("fsh_65CrKEyQM7ePE0X7PtzKBR", bounds=bounds, res=h3_size) # USGS
    # df_dem = fused.run("fsh_65CrKEyQM7ePE0X7PtzKBR", bounds=bounds, res=h3_size) # USGS dem_10meter_tile_hex_2
    # df = duckdb.sql(" select df.*, df_dem.metric from df left join df_dem on df.hex = df_dem.hex").df()
    # df['metric'] = df["metric"] - 3500
    # df = df[df["metric"]>70]
    print(df)
    return df