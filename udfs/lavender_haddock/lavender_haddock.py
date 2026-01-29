@fused.udf
def udf(bounds: fused.types.Tile,
        res: int = 10,
        *, chip_len=256):
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    import numpy as np
    import pandas as pd
    import rioxarray
    
    # List of the three files to process
    file_paths = [
    's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone1_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone2_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone3_2021.tif',  # Not shown in screenshot
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone4_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone5_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone6_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone7_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone8_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone9_2021.tif',  # Not shown in screenshot
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone10_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone11_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone12_2021.tif',  # Not shown in screenshot
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone13_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone14_2021.tif',
    # 's3://fused-users/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone15_2021.tif'
]
    def gather(file_paths, res, chip_len, bounds):
        all_dfs = []
        dem_hex_utils = fused.load("https://github.com/fusedio/udfs/tree/0b1bd10/public/DEM_Tile_Hexify/").utils
        
        for path in file_paths:
            # Read the raw data
            arr = rioxarray.open_rasterio(path).squeeze(drop=True).rio.reproject("EPSG:4326")
            print(arr)
            bounds = bounds.bounds.values[0]
            
            df = utils.arr_to_latlng(arr, bounds)
            df = dem_hex_utils.aggregate_df_hex(df=df, res=res)
            df = df[df['metric'] < 4857]
            
            all_dfs.append(df)
        
        # Combine all dataframes
        if all_dfs:
            return pd.concat(all_dfs)
        else:
            return None
    df = gather(file_paths, res, chip_len, bounds)
    print(df)
    return df