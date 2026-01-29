@fused.udf
def udf(bounds: fused.types.Bounds=None, bucket_path: str="gs://fused-fd-stephenkentdata/stephenkentdata/DEM_from_USGS/grand_canyon/**/*.tif"):
    """
    Processes COG files: clips to bounds, reprojects, converts to dataframe.
    Returns: Combined DataFrame with x, y, data columns
    """
    import rioxarray
    import pandas as pd
    import numpy as np
    utils = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    
    file_paths = fused.run("fsh_5j1WW9a1FNUxAx7o7D5Byc", bounds=bounds, bucket_path=bucket_path)
    
    if file_paths is None:
        print("No file paths provided")
        return None
    
    bounds_gdf = utils.bounds_to_gdf(bounds)
    all_dfs = []
    
    for path in file_paths:
        try:
            print(f"\n{'='*60}")
            print(f"Processing: {path.split('/')[-1]}")
            
            with rioxarray.open_rasterio(path, chunks='auto') as da_tiff:
                print(f"  Original shape: {da_tiff.shape}")
                print(f"  Original CRS: {da_tiff.rio.crs}")
                print(f"  Data type: {da_tiff.dtype}")
                print(f"  NoData value: {da_tiff.rio.nodata}")
                
                # Check original data range
                sample_data = da_tiff.isel(band=0).values
                print(f"  Original data range: [{np.nanmin(sample_data):.2f}, {np.nanmax(sample_data):.2f}]")
                print(f"  Original data mean: {np.nanmean(sample_data):.2f}")
                print(f"  Count of zeros: {np.sum(sample_data == 0)}")
                print(f"  Count of valid (non-NaN): {np.sum(~np.isnan(sample_data))}")
                
                bounds_geom_native = bounds_gdf.to_crs(da_tiff.rio.crs)
                da_clipped = da_tiff.rio.clip(bounds_geom_native.geometry, drop=True)
                
                if da_clipped.size == 0:
                    print("  ⚠ Clipped data is empty!")
                    continue
                
                print(f"  Clipped shape: {da_clipped.shape}")
                clipped_data = da_clipped.isel(band=0).values
                print(f"  Clipped data range: [{np.nanmin(clipped_data):.2f}, {np.nanmax(clipped_data):.2f}]")
                
                da_4326 = da_clipped.squeeze(drop=True).rio.reproject("EPSG:4326")
                print(f"  Reprojected shape: {da_4326.shape}")
                reprojected_data = da_4326.values
                print(f"  Reprojected data range: [{np.nanmin(reprojected_data):.2f}, {np.nanmax(reprojected_data):.2f}]")
                
                df_tiff = da_4326.to_dataframe("data").reset_index()[["y", "x", "data"]]
                print(f"  Initial DataFrame: {len(df_tiff)} rows")
                print(f"  Data column stats before filtering:")
                print(f"    Min: {df_tiff['data'].min():.2f}")
                print(f"    Max: {df_tiff['data'].max():.2f}")
                print(f"    Mean: {df_tiff['data'].mean():.2f}")
                print(f"    Zeros: {(df_tiff['data'] == 0).sum()}")
                
                df_tiff = df_tiff.dropna()
                print(f"  After dropna: {len(df_tiff)} rows")
                
                df_tiff = df_tiff[df_tiff['data'] > -9999]
                print(f"  After filtering > -9999: {len(df_tiff)} rows")
                
                if len(df_tiff) > 0:
                    print(f"  ✓ Final: {len(df_tiff)} points")
                    print(f"  Sample values: {df_tiff['data'].head(10).tolist()}")
                    all_dfs.append(df_tiff)
                    
        except Exception as e:
            print(f"✗ Error processing {path}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f"\n{'='*60}")
        print(f"FINAL COMBINED RESULTS:")
        print(f"  Total points: {len(combined_df)}")
        print(f"  Data range: [{combined_df['data'].min():.2f}, {combined_df['data'].max():.2f}]")
        print(f"  Data mean: {combined_df['data'].mean():.2f}")
        print(combined_df.head(20))
        # return combined_df
        return None
    else:
        print("No data loaded")
        return None