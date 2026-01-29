def udf(bounds: fused.types.Bounds=None, bucket_path: str= "gs://fused-fd-stephenkentdata/stephenkentdata/DEM_from_USGS/grand_canyon/**/*.tif"):
    import rasterio
    from rasterio.env import Env
    from rasterio.warp import transform_bounds
    import fsspec
    from shapely.geometry import box
    utils = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    
    fs = fsspec.filesystem('gcs')
    all_files = fs.glob(bucket_path)
    file_paths = [f"gs://{f}" for f in all_files]
    
    print(f"Found {len(file_paths)} total files")
    print("=" * 80)
    
    bounds_gdf = utils.bounds_to_gdf(bounds)
    bounds_geom = bounds_gdf.geometry.iloc[0]
    valid_files = []
    
    with Env(GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR'):
        for path in file_paths:
            try:
                with rasterio.open(path) as src:
                    # Transform file bounds to WGS84
                    file_bounds_wgs84 = transform_bounds(
                        src.crs,
                        'EPSG:4326',
                        *src.bounds
                    )
                    file_box = box(*file_bounds_wgs84)
                    intersects = bounds_geom.intersects(file_box)
                    
                    # Gather detailed file info
                    status = "✓ MATCH" if intersects else "✗ SKIP"
                    filename = path.split('/')[-1]
                    
                    print(f"\n{status}: {filename}")
                    print(f"  CRS: {src.crs}")
                    print(f"  Dimensions: {src.width} x {src.height} pixels")
                    print(f"  Bands: {src.count}")
                    print(f"  Data type: {src.dtypes[0]}")
                    print(f"  Resolution: {src.res[0]:.6f} x {src.res[1]:.6f} (units of CRS)")
                    print(f"  Bounds (native): {src.bounds}")
                    print(f"  Bounds (WGS84): {file_bounds_wgs84}")
                    print(f"  NoData value: {src.nodata}")
                    
                    # Get band statistics if available
                    try:
                        stats = src.statistics(1)
                        print(f"  Band 1 stats - Min: {stats.min:.2f}, Max: {stats.max:.2f}, Mean: {stats.mean:.2f}")
                    except:
                        pass
                    
                    if intersects:
                        valid_files.append(path)
                        
            except Exception as e:
                print(f"\n✗ ERROR: {path.split('/')[-1]}")
                print(f"  {type(e).__name__}: {e}")
                continue
    
    print("\n" + "=" * 80)
    print(f"SUMMARY: Filtered to {len(valid_files)} files that intersect bounds")
    return valid_files