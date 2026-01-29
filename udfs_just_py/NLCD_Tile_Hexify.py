@fused.udf
def udf(bounds: fused.types.Bounds, 
        year:int=2020, 
        land_type:str='', 
        chip_len:int=256,
       res: int = 12):
    import numpy as np        
    import pandas as pd
    from utils import get_data, arr_to_h3, nlcd_category_dict, rgb_to_hex

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    #initial parameters
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    res_offset = 1  # lower makes the hex finer
    # res = max(min(int(3 + zoom / 1.5), 12) - res_offset, 2)
    # res = 10
    print(res)
    
    # read tiff file
    arr_int, color_map = get_data(tile, year, land_type, chip_len)

    # hexify tiff array
    
    df = arr_to_h3(arr_int, bounds, res=res, ordered=False)

    # find most frequet land_type
    df['most_freq'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[0][np.argmax(np.unique(x, return_counts=True)[1])])
    df['n_pixel'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[1].max())
    df=df[df['most_freq']>0]
    if len(df)==0: return 

    # get the color and land_type
    df[['r', 'g', 'b', 'a']] = df.most_freq.map(lambda x: pd.Series(color_map[x])).apply(pd.Series)
    df['land_type'] = df.most_freq.map(nlcd_category_dict)
    df['color'] = df.most_freq.map(lambda x: rgb_to_hex(color_map[x]) if x in color_map else "NaN")
    df_dem = fused.run("fsh_6N48YfmXRRWisQcAwnGYlh", bounds=bounds, res=res) #NH USGS
    print(df_dem)
    df = df.merge(df_dem[['hex', 'metric']], on='hex', how='inner')
    df = df.rename(columns={'metric': 'elevation'})
    #print the stats for each tiles
    print(df.groupby(['color','land_type'])['n_pixel'].sum().sort_values(ascending=False))
    print(df.columns)
    return df