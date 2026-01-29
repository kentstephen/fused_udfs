@fused.udf
def udf(bbox: fused.types.TileGDF = None,
        chip_len=256,
        res: int= 7,
    path: str='s3://fused-users/stephenkentdata/egy_pd_2020_1km_UNadj.tif'):
    import numpy as np
    import rasterio
    import matplotlib.pyplot as plt
    import duckdb
    import pandas as pd
    from utils import add_rgb
    bounds = bbox.bounds.values[0] 
    utils = fused.load('https://github.com/fusedio/udfs/tree/004b8d9/public/common/').utils
    arr = utils.read_tiff(bbox, path, output_shape=(chip_len, chip_len))
    
    # Ensure it's a writable copy and handle NaNs
    arr = np.array(arr, copy=True)
    arr = np.nan_to_num(arr, nan=0.0)
    
    # Normalize to [0, 255] for display
    arr = (arr / np.max(arr) * 255).astype(np.uint8)
    
    
    arr_to_h3 = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_h3
    df = arr_to_h3(arr, bounds=bounds, res=res)
    # print(img_df)
  # con = fused.utils.common.duckdb_connect()
    
    # Step 1: Sum the elements in each 'agg_data' array and store in a new column 'pop'
    df['pop'] = df['agg_data'].apply(lambda x: sum(x))
    
    # Step 2: Drop the 'agg_data' column since it's no longer needed
    df.drop(columns=['agg_data'], inplace=True)
    
   


    df = add_rgb(df, 'pop')
    print(df)
    
    return df


