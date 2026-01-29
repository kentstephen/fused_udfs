# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf(cache_max_age="0s")
def udf(bounds: fused.types.Bounds=None, 
        time_of_interest="2023-07-01/2023-08-30", 
        chip_len:int=256, 
        scale:float=0.075,
       res: int = 12):
    import geopandas as gpd
    import shapely
    import pandas as pd
    import numpy as np
    import duckdb
    from utils import get_arr, tile_to_df, df_to_hex,road_cmap

    # Using common fused functions as helper
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/482f6de/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds)

    
    # # find the tiles with intersecting geom
    # gdf = gpd.read_file('s3://fused-asset/data/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
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
    # res = min(int(3+zoom/1.5),15)
    print(res) 
    data_cols = [f'band{i+1}' for i in range(len(arr))]
    bounds_gdf = common_utils.bounds_to_gdf(bounds)
    bounds_values = bounds_gdf.bounds.values[0]
    df = df_to_hex(bounds_values, df, data_cols=data_cols, res=res, hex_col='hex', return_avg_lalng=False)
    print(df)

    # calculate stats: mean pixel value for each hex
    mask=1
    for col in data_cols:
        df[f'agg_{col}']=df[f'agg_{col}'].map(lambda x:x.mean())
        mask=mask*df[f'agg_{col}']>0
    df = df[mask]
    # return df

    # convert the h3_int to h3_hex
    # df['hex'] = df['hex'].map(lambda x:hex(x)[2:])
    # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=bounds, h3_size=res) # hexify
    # print(df_dem['hex'])
    df_dem = fused.run("fsh_65CrKEyQM7ePE0X7PtzKBR", bounds=bounds, res=res) # USGS
    df = duckdb.sql("select df.*, df_dem.metric from df inner join df_dem on df.hex = df_dem.hex").df()
    # return df
    df_road = fused.run("fsh_6N6n5fHUn7U1g2MLsM0w45", bounds=bounds, res=res)
    if df_road is not None and not df_road.empty:
        # Get unique hex values from df_road
        road_hex_values = df_road['hex'].unique()
        
        # Apply colors based on road class for each hex value
        for hex_value in road_hex_values:
            # Find all rows in df_road with this hex value
            road_classes = df_road.loc[df_road['hex'] == hex_value, 'class'].unique()
            
            # If there are multiple classes for one hex, use the first one
            road_class = road_classes[0] if len(road_classes) > 0 else 'unknown'
            
            # Get the color for this road class (default to gray if not in the map)
            color = road_cmap.get(road_class, (128, 128, 128))
            
            # Apply the color to all rows in df with this hex value
            mask = df['hex'] == hex_value
            df.loc[mask, 'agg_band1'] = color[0]
            df.loc[mask, 'agg_band2'] = color[1]
            df.loc[mask, 'agg_band3'] = color[2]
    # df['metric'] = df["metric"] - 1400  
    # df = df[df["metric"]>70]
    print(df)
    return df