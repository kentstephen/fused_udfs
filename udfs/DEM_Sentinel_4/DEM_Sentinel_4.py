@fused.udf
def udf(bounds: fused.types.Bounds=None,
        time_of_interest="2024-06-25/2024-08-15", 
        chip_len:int=768, 
        scale:float=0.0571,
        path: str='s3://fused-users/stephenkentdata/2025/whitemountains/wmnf_boundary.zip'):    
    import geopandas as gpd
    import shapely
    import pandas as pd
    import numpy as np
    import duckdb
    from utils import tile_to_df, df_to_hex

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    
    # # # find the tiles with intersecting geom
    # gdf = gpd.read_file(path)

    # gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(100_000).to_crs('EPSG:4326')
    # gdf_clipped = gdf.dissolve().to_crs(4326).clip(tile)
    
    # gdf_w_bounds = pd.concat([gdf_clipped,tile])
    # if len(gdf_w_bounds)<=1:
    #     print('No bounds is intersecting with the given geometry.')
    #     return 
        
    # read sentinel data
    udf_sentinel = fused.load('https://github.com/fusedio/udfs/tree/a1c01c6/public/DC_AOI_Example/')
    arr = udf_sentinel.utils.get_arr(tile, time_of_interest=time_of_interest, output_shape=(chip_len, chip_len), max_items=100)
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
    h3_size=11
    print(h3_size) 
    data_cols = [f'band{i+1}' for i in range(len(arr))]
    df = df_to_hex(df, data_cols=data_cols, h3_size=h3_size, hex_col='hex', return_avg_lalng=True)

    # calculate stats: mean pixel value for each hex
    mask=1
    for col in data_cols:
        df[f'agg_{col}']=df[f'agg_{col}'].map(lambda x:x.mean())
        mask=mask*df[f'agg_{col}']>0
    df = df[mask]

    # convert the h3_int to h3_hex
    # df['hex'] = df['hex'].map(lambda x:hex(x)[2:])
    
    
    df_dem = fused.run("fsh_1aai0G9A4z1vau0sxGdclj", bounds=bounds, h3_size=h3_size)
    
    if df_dem is None or df_dem.empty:
        return
    df = duckdb.sql("select df.*, df_dem.metric, df_dem.elev_scale from df inner join df_dem on df.hex = df_dem.hex").df()
    # df = df.merge(df_dem[['hex', 'metric']], on='hex', how='left')
    print(df)
    return df




    