utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils

@fused.udf
def udf(bounds: fused.types.Bounds=None, file_paths:list=None):
   
    import rioxarray
    import pandas as pd
    import fsspec
    from shapely.geometry import box
    
    fs = fsspec.filesystem('gcs')
    
    # Use glob with full path including bucket
    all_files = fs.glob('gs://fused-fd-stephenkentdata/stephenkentdata/stephenkentdata/snake_river_wy_1m_dem/**/*.tif')
    file_paths = [f"gs://{f}" for f in all_files]
    
    print(f"Found {len(file_paths)} files")
    
    # Convert bounds to geometry for intersection tests
    bounds_gdf = utils.bounds_to_gdf(bounds)
    bounds_geom = bounds_gdf.geometry.iloc[0]
    
    # Filter files by spatial extent BEFORE loading
    valid_files = []
    for path in file_paths:
        try:
            # Just open metadata, don't load data
            with rioxarray.open_rasterio(path, chunks='auto') as da_tiff:
                # Get file bounds in EPSG:4326
                file_bounds = da_tiff.rio.transform_bounds("EPSG:4326")
                file_box = box(file_bounds[0], file_bounds[1], file_bounds[2], file_bounds[3])
                
                # Check intersection
                if bounds_geom.intersects(file_box):
                    valid_files.append(path)
                else:
                    print(f"Skipping {path} - no intersection")
                    
        except Exception as e:
            print(f"Error checking bounds for {path}: {e}")
            continue
    
    print(f"Filtered to {len(valid_files)} files that intersect bounds")
    
    # Now process only the valid files
    all_dfs = []
    for path in valid_files:
        try:
            with rioxarray.open_rasterio(path, chunks='auto') as da_tiff:
                bounds_geom_native = bounds_gdf.to_crs(da_tiff.rio.crs)
                da_clipped = da_tiff.rio.clip(bounds_geom_native.geometry, drop=True)
                
                if da_clipped.size == 0:
                    continue
                
                da_4326 = da_clipped.squeeze(drop=True).rio.reproject("EPSG:4326")
                df_tiff = da_4326.to_dataframe("data").reset_index()[["y", "x", "data"]]
                
                df_tiff = df_tiff.dropna()
                df_tiff = df_tiff[df_tiff['data'] > -9999]
                
                if len(df_tiff) > 0:
                    print(f"Loaded {len(df_tiff)} points from {path}")
                    all_dfs.append(df_tiff)
                
        except Exception as e:
            print(f"Error processing {path}: {e}")
            continue
    
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f"Total combined points: {len(combined_df)}")
        return combined_df
    else:
        print("No data loaded from any files")
        return None