@fused.udf(cache_max_age="0s")
def udf(bounds: fused.types.Tile,
        res: int = 10,
        *, chip_len=256):
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    import numpy as np
    import pandas as pd
    
    # List of the three files to process
    file_paths = [
        's3://fused-users/stephenkentdata/DEM_from_USGS/denali/USGS_AK5M_AK_IFSAR_2010_37.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/denali/USGS_AK5M_AK_IFSAR_2010_38.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/denali/USGS_AK5M_AK_IFSAR_2010_57.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/denali/USGS_AK5M_AK_IFSAR_2010_58.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/denali/USGS_AK5M_AK_IFSAR_2010_60.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/denali/USGS_AK5M_AK_IFSAR_2010_77.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/denali/USGS_AK5M_AK_IFSAR_2010_80.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/denali/USGS_AK5M_AK_IFSAR_2010_73.tif'
    ]
    def gather(file_paths, res, chip_len, bounds):
        all_dfs = []
        dem_hex_utils = fused.load("https://github.com/fusedio/udfs/tree/0b1bd10/public/DEM_Tile_Hexify/").utils
        
        for path in file_paths:
            # Read the raw data
            arr = utils.read_tiff(bounds, path, output_shape=(chip_len, chip_len))
            if arr is None:
                continue
                
            # Convert to UInt16 to satisfy the PNG driver requirement
            arr = arr.astype(np.uint16)
            bounds_val = bounds.bounds.values[0]
            
            df = utils.arr_to_latlng(arr, bounds_val)
            df = dem_hex_utils.aggregate_df_hex(df=df, res=res)
            df = df[df['metric'] < 4857]
            
            all_dfs.append(df)
        
        # Combine all dataframes
        if all_dfs:
            return pd.concat(all_dfs)
        else:
            return None
    df = gather(file_paths, res, chip_len, bounds)
    return df