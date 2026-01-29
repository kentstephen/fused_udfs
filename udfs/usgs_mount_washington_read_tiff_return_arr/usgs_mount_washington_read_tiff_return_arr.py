utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils

@fused.udf
def udf(bounds: fused.types.Bounds):
    import rioxarray
    import pandas as pd
    import numpy as np
    
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    
    file_paths = [
        's3://fused-users/stephenkentdata/DEM_from_USGS/mount_washington_1m/USGS_one_meter_x31y491_NH_CT_RiverNorthL6_P2_2015.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/mount_washington_1m/USGS_1M_19_x32y491_NH_Umbagog_LiDAR_2016_D16.tif',
        's3://fused-users/stephenkentdata/DEM_from_USGS/mount_washington_1m/USGS_1M_19_x31y491_NH_Umbagog_LiDAR_2016_D16.tif'
    ]
    
    def process_single_tiff(path, bounds=bounds):
        try:
            with rioxarray.open_rasterio(path) as da_tiff:
                bounds_geom = utils.bounds_to_gdf(bounds).to_crs(da_tiff.rio.crs)
                da_clipped = da_tiff.rio.clip(bounds_geom.geometry, drop=True)
                
                if da_clipped.size == 0:
                    print(f"No data in bounds for {path}")
                    return None
                
                da_4326 = da_clipped.squeeze(drop=True).rio.reproject("EPSG:4326")
                
                data_array = da_4326.values
                data_array = data_array[~np.isnan(data_array)]
                data_array = data_array[data_array > -9999]
                
                if len(data_array) == 0:
                    print(f"No valid data after filtering for {path}")
                    return None
                    
                print(f"Loaded {len(data_array)} points from {path}")
                return data_array
                
        except Exception as e:
            print(f"Error processing {path}: {e}")
            return None
    
    print(f"Starting parallel processing of {len(file_paths)} files...")
    processed_arrays = common.run_pool(process_single_tiff, file_paths)
    print(f"Parallel processing completed. Got {len(processed_arrays)} results.")
    
    valid_arrays = [arr for arr in processed_arrays if arr is not None and len(arr) > 0]
    print(f"Valid arrays: {len(valid_arrays)} out of {len(processed_arrays)}")
    
    for i, arr in enumerate(processed_arrays):
        if arr is not None and len(arr) > 0:
            print(f"File {i+1}: {len(arr)} points loaded successfully")
        else:
            print(f"File {i+1}: No data or failed to process")
    
    if valid_arrays:
        combined_array = np.concatenate(valid_arrays)
        print(f"Total combined points: {len(combined_array)}")
        return combined_array.astype("uint8")
    else:
        print("No data loaded from any files")
        return None