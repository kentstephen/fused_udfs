@fused.udf
def udf(bounds: fused.types.Bounds=None):
    import rasterio
    from rasterio.env import Env
    import numpy as np
    import fsspec
    
    file_path = "gs://fused-fd-stephenkentdata/stephenkentdata/DEM_from_USGS/grand_canyon/DEME_Zone5_2021.tif"
    
    print(f"Analyzing: {file_path.split('/')[-1]}")
    print("=" * 80)
    
    fs = fsspec.filesystem('gcs')
    
    with Env(GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR'):
        with fs.open(file_path, 'rb') as f:
            with rasterio.open(f) as src:
                # Basic file info
                print(f"\nFILE METADATA:")
                print(f"  CRS: {src.crs}")
                print(f"  Dimensions: {src.width} x {src.height} pixels")
                print(f"  Bands: {src.count}")
                print(f"  Data type: {src.dtypes[0]}")
                print(f"  Resolution: {src.res}")
                print(f"  Bounds: {src.bounds}")
                print(f"  NoData value: {src.nodata}")
                
                # Just read a small window instead of entire file
                print(f"\nDATA ANALYSIS (sampling middle 1000x1000 pixels):")
                center_y, center_x = src.height // 2, src.width // 2
                window = ((center_y - 500, center_y + 500), (center_x - 500, center_x + 500))
                data = src.read(1, window=window)
                
                print(f"  Sample pixels: {data.size:,}")
                print(f"  Min: {np.min(data):.2f}")
                print(f"  Max: {np.max(data):.2f}")
                print(f"  Mean: {np.mean(data):.2f}")
                
                zero_count = np.sum(data == 0)
                print(f"  Zeros: {zero_count:,} ({100*zero_count/data.size:.1f}%)")
                print(f"  Non-zeros: {np.sum(data > 0):,}")