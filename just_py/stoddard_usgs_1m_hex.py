utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds,
        res: int = 14,
        stats_type: str = "mean",
       
):
   
    import numpy as np
    import pandas as pd
    import rioxarray
    
    # List of the DEM files to process

    zoom = utils.estimate_zoom(bounds)
    try:
        df = hexagons(res, stats_type, bounds)
    
        # df["elev_scale"] = int((15 - zoom) * 1)
        print(df)
        return df
        
    except Exception as e:
        return None
   

# @fused.cache
# def read_tiff(file_paths, bounds):
#     import rioxarray
#     import pandas as pd
    
#     all_dfs = []
    
#     for path in file_paths:
#         try:
#             # Clip to bounds BEFORE loading full file
#             with rioxarray.open_rasterio(path) as da_tiff:
#                 # Get the bounds in the file's native CRS first for clipping
#                 bounds_geom = utils.bounds_to_gdf(bounds).to_crs(da_tiff.rio.crs)
                
#                 # Clip to the bounds area in native CRS
#                 da_clipped = da_tiff.rio.clip(bounds_geom.geometry, drop=True)
                
#                 if da_clipped.size == 0:
#                     print(f"No data in bounds for {path}")
#                     continue
                
#                 # NOW reproject to EPSG:4326 (WGS84) and convert to dataframe
#                 da_4326 = da_clipped.squeeze(drop=True).rio.reproject("EPSG:4326")
#                 df_tiff = da_4326.to_dataframe("data").reset_index()[["y", "x", "data"]]
                
#                 # Remove no-data values
#                 df_tiff = df_tiff.dropna()
#                 df_tiff = df_tiff[df_tiff['data'] > -9999]  # Remove typical no-data values
                
#                 if len(df_tiff) == 0:
#                     print(f"No valid data after filtering for {path}")
#                     continue
                    
#                 print(f"Loaded {len(df_tiff)} points from {path}")
#                 all_dfs.append(df_tiff)
                
#         except Exception as e:
#             print(f"Error processing {path}: {e}")
#             continue
    
#     # Combine all dataframes
#     if all_dfs:
#         combined_df = pd.concat(all_dfs, ignore_index=True)
#         print(f"Total combined points: {len(combined_df)}")
#         return combined_df
#     else:
#         print("No data loaded from any files")
#         return None
@fused.cache
def hexagons(res, stats_type, bounds):
    dem_hex = fused.load("https://github.com/fusedio/udfs/tree/0b1bd10/public/DEM_Tile_Hexify/")
    
    # Read and combine all TIFF files
    df_tiff = fused.run('fsh_ZVodDscs1BEmh3C4pXlTu', bounds=bounds)
    print(f"df_tiff:{df_tiff}")
    
    if df_tiff is None or df_tiff.empty:
        return None
        
    print(f"Combined dataframe shape: {df_tiff.shape}")
    
    # Hexagonify & aggregate
    df = dem_hex.utils.aggregate_df_hex(
        df_tiff, res, latlng_cols=["y", "x"], stats_type=stats_type
    )
    
    if df is None or df.empty:
        return None
    del df['agg_data']  
    # Clean up columns if needed
    # if 'agg_data' in df.columns:
    #     df = df.drop('agg_data', axis=1)
    
    # Apply elevation offset if needed
    # df['metric'] = df['metric'] - 1500
    
    return df
    # def gather(file_paths, res, chip_len, bounds, tile):
    #     all_dfs = []
    #     dem_hex_utils = fused.load("https://github.com/fusedio/udfs/tree/0b1bd10/public/DEM_Tile_Hexify/").utils
        
    #     for path in file_paths:
    #         # Read the raw data
    #         arr = utils.read_tiff(tile, path, output_shape=(chip_len, chip_len))
    #         if arr is None:
    #             return None
                
    #         # Convert to UInt16 to satisfy the PNG driver requirement
    #         arr = arr.astype(np.uint16)

    #         # bounds_val = bounds.bounds.values[0]
            
    #         df = utils.arr_to_latlng(arr, bounds)
    #         df = dem_hex_utils.aggregate_df_hex(df=df, res=res)
    #         # df = df[df['metric'] < 4857]
    #         del df['agg_data']
            
    #         all_dfs.append(df)
        
    #     # Combine all dataframes
    #     if all_dfs:
    #         return pd.concat(all_dfs)
    #     else:
    #         return None
    # df = gather(file_paths, res, chip_len, bounds, tile)
    # if df is None or df.empty:
    #     return
    # df['metric'] = df["metric"] - 1500
    # return df