@fused.udf
def udf(bounds: fused.types.Bounds=None):
    import rioxarray
    import numpy as np
    
    file_path = "gs://fused-fd-stephenkentdata/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone5_2021.tif"
    
    print(f"Analyzing: {file_path.split('/')[-1]}")
    print("=" * 80)
    
    # Open with chunks to avoid loading entire file
    with rioxarray.open_rasterio(file_path, chunks={'x': 512, 'y': 512}) as da:
        print(f"\nFILE METADATA:")
        print(f"  CRS: {da.rio.crs}")
        print(f"  Shape: {da.shape}")
        print(f"  Bounds: {da.rio.bounds()}")
        print(f"  NoData: {da.rio.nodata}")
        
        # Read just a tiny 100x100 sample from the middle
        # print(f"\nSampling 100x100 pixels from center...")
        # center_y = da.shape[1] // 2
        # center_x = da.shape[2] // 2
        
        # sample = da.isel(band=0, y=slice(center_y, center_y+100), x=slice(center_x, center_x+100)).values
        
        # print(f"  Sample size: {sample.shape}")
        # print(f"  Min: {np.min(sample):.2f}")
        # print(f"  Max: {np.max(sample):.2f}")
        # print(f"  Mean: {np.mean(sample):.2f}")
        # print(f"  Zeros: {np.sum(sample == 0)} ({100*np.sum(sample == 0)/sample.size:.1f}%)")
        # print(f"\n  Sample values (5x5):")
        # print(sample[:5, :5])