def udf(bounds: fused.types.Bounds=None, bucket_path: str='gs://fused-fd-stephenkentdata/stephenkentdata/DEM_from_USGS/denali/**/*.tif'):
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

                    if bounds_geom.intersects(file_box):
                        valid_files.append(path)
                        print(f"✓ {path.split('/')[-1]}")
                    else:
                        print(f"✗ {path.split('/')[-1]}")

            except Exception as e:
                print(f"Error checking {path.split('/')[-1]}: {e}")
                continue

    print(f"\nFiltered to {len(valid_files)} files that intersect bounds")
    return valid_files