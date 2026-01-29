@fused.udf
def udf(bucket_path: str='gs://fused-fd-stephenkentdata/stephenkentdata/DEM_from_USGS/denali/**/*.tif'):
    """
    Investigates COG files to understand their structure and naming.
    """
    import fsspec
    import rasterio
    from rasterio.env import Env
    
    fs = fsspec.filesystem('gcs')
    all_files = fs.glob(bucket_path)
    file_paths = [f"gs://{f}" for f in all_files]
    
    print(f"FOUND {len(file_paths)} FILES")
    print("")
    
    print("FILENAMES:")
    for i, path in enumerate(file_paths, 1):
        filename = path.split('/')[-1]
        print(f"  {i}. {filename}")
    
    print("")
    print("CHECKING FIRST FILE...")
    
    if file_paths:
        first_file = file_paths[0]
        print(f"File: {first_file.split('/')[-1]}")
        
        try:
            with Env(GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR'):
                with rasterio.open(first_file) as src:
                    print(f"CRS: {src.crs}")
                    print(f"Bounds (native): {src.bounds}")
                    
                    bounds_wgs84 = rasterio.warp.transform_bounds(
                        src.crs,
                        'EPSG:4326',
                        *src.bounds
                    )
                    print(f"Bounds (WGS84): {bounds_wgs84}")
                    print(f"  Longitude: {bounds_wgs84[0]:.4f} to {bounds_wgs84[2]:.4f}")
                    print(f"  Latitude: {bounds_wgs84[1]:.4f} to {bounds_wgs84[3]:.4f}")
                    
        except Exception as e:
            print(f"ERROR: {e}")