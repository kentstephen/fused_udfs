utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils

@fused.udf#(cache_max_age="0s")  # Temporarily disable caching
def udf(bounds: fused.types.Bounds):
    import rioxarray
    import pandas as pd
    import numpy as np
    
    # Load common utilities for run_pool
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    
    file_paths = [
        's3://fused-users/stephenkentdata/DEM_from_USGS/mount_washington_1m/USGS_one_meter_x31y491_NH_CT_RiverNorthL6_P2_2015.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/mount_washington_1m/USGS_1M_19_x32y491_NH_Umbagog_LiDAR_2016_D16.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/mount_washington_1m/USGS_1M_19_x31y491_NH_Umbagog_LiDAR_2016_D16.tif'
    ]
    
    # @fused.cache  # Disabled to avoid cached None results
    def process_single_tiff(path, bounds=bounds):
        """Process a single TIFF file and return a DataFrame"""
        try:
            # Clip to bounds BEFORE loading full file
            with rioxarray.open_rasterio(path) as da_tiff:
                # Get the bounds in the file's native CRS first for clipping
                bounds_geom = utils.bounds_to_gdf(bounds).to_crs(da_tiff.rio.crs)
                
                # Clip to the bounds area in native CRS
                da_clipped = da_tiff.rio.clip(bounds_geom.geometry, drop=True)
                
                if da_clipped.size == 0:
                    print(f"No data in bounds for {path}")
                    return None
                
                # NOW reproject to EPSG:4326 (WGS84) and convert to dataframe
                da_4326 = da_clipped.squeeze(drop=True).rio.reproject("EPSG:4326")
                df_tiff = da_4326.to_dataframe("data").reset_index()[["y", "x", "data"]]
                
                # Remove no-data values
                df_tiff = df_tiff.dropna()
                df_tiff = df_tiff[df_tiff['data'] > -9999]  # Remove typical no-data values
                
                if len(df_tiff) == 0:
                    print(f"No valid data after filtering for {path}")
                    return None
                    
                print(f"Loaded {len(df_tiff)} points from {path}")
                return df_tiff
                
        except Exception as e:
            print(f"Error processing {path}: {e}")
            return None
    
    # Use run_pool to process all TIFF files in parallel
    print(f"Starting parallel processing of {len(file_paths)} files...")
    processed_dfs = common.run_pool(process_single_tiff, file_paths)
    print(f"Parallel processing completed. Got {len(processed_dfs)} results.")
    
    # Filter out None results and combine valid dataframes
    valid_dfs = [df for df in processed_dfs if df is not None and len(df) > 0]
    print(f"Valid dataframes: {len(valid_dfs)} out of {len(processed_dfs)}")
    
    # Print info about each result
    for i, df in enumerate(processed_dfs):
        if df is not None and len(df) > 0:
            print(f"File {i+1}: {len(df)} points loaded successfully")
        else:
            print(f"File {i+1}: No data or failed to process")
    
    if valid_dfs:
        combined_df = pd.concat(valid_dfs, ignore_index=True)
        print(f"Total combined points: {len(combined_df)}")
        print(f"Combined dataframe shape: {combined_df.shape}")
        print(combined_df)
        return combined_df
    else:
        print("No data loaded from any files")
        return None