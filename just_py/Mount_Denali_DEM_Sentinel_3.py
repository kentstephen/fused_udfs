# Go to Alaska, north of Anchorage
@fused.udf
def udf(bounds: fused.types.Bounds=None,
        time_of_interest="2021-07-15/2021-09-30", 
        chip_len:int=768, 
        scale:float=0.0571,
        h3_size: int = 11): 

    import geopandas as gpd
    import shapely
    import pandas as pd
    import numpy as np
    import duckdb
    from utils import get_arr, tile_to_df, df_to_hex

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)
    
    @fused.cache
    def get_boundary():
        gdf = gpd.read_file('http://gstore.unm.edu/apps/rgisarchive/datasets/7bbe8af5-029b-4adf-b06c-134f0dd57226/nps_boundary.original.zip')
        return gdf[gdf['UNIT_CODE']=='DENA']
    gdf = get_boundary()
    # gdf = gdf.dissolve()
    # print(gdf.columns)
    # return gdf
    # gdf_clipped = gdf.dissolve().to_crs(4326).clip(tile)
    
    # gdf_w_bounds = pd.concat([gdf_clipped,tile])
    # if len(gdf_w_bounds)<=1:
    #     print('No bounds is intersecting with the given geometry.')
    #     return 
        
    # read sentinel data
    # udf_sentinel_utils = fused.load('https://github.com/fusedio/udfs/tree/a1c01c6/public/DC_AOI_Example/').utils
    arr = get_arr(tile, time_of_interest=time_of_interest, output_shape=(chip_len, chip_len), max_items=1000)
    if arr is None:
        return
    arr = np.clip(arr *  scale, 0, 255).astype("uint8")[:3]
    
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # create a geom mask
    
    # geom_mask = utils.gdf_to_mask_arr(gdf_w_bounds, arr.shape[-2:], first_n=1)    
    
    # arr = np.ma.masked_array(arr, [geom_mask]*arr.shape[0])
    
    # convert arr to xyz dataframe
    df = tile_to_df(tile, arr)
    # h3_size = min(int(3+zoom/1.5),15)
    
    print(h3_size) 
    data_cols = [f'band{i+1}' for i in range(len(arr))]
    df = df_to_hex(df, data_cols=data_cols, h3_size=h3_size, hex_col='hex', return_avg_lalng=False)

    # calculate stats: mean pixel value for each hex
    mask=1
    for col in data_cols:
        df[f'agg_{col}']=df[f'agg_{col}'].map(lambda x:x.mean())
        mask=mask*df[f'agg_{col}']>0
    df = df[mask]

    # convert the h3_int to h3_hex
    # df['hex'] = df['hex'].map(lambda x:hex(x)[2:])
    @fused.cache
    def dem(bounds, h3_size):
        return fused.run("fsh_3M3RyItkeAZpGR6fMZ482r", bounds=bounds, res=h3_size) #alos
    df_dem = dem(bounds=bounds, h3_size=h3_size)
    if df_dem is None or df_dem.empty:
        return
    df = duckdb.sql("select df.*, df_dem.metric from df left join df_dem on df.hex = df_dem.hex").df()
    # df = df.merge(df_dem[['hex', 'metric']], on='hex', how='left')
    print(df)
    # df = duckdb.sql("install h3 from community; load h3; select h3_h3_to_string(hex) as hex, * exclude(hex) from df").df()
    # df = df[df['metric']<8000]
    return df