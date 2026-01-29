@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds=None, file_paths: list=None):
    """
    Processes COG files: clips to bounds, reprojects, converts to dataframe.
    Returns: Combined DataFrame with x, y, data columns
    """

    import rioxarray
    import pandas as pd

    # utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/')
    utils = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    file_paths = fused.run("fsh_7k40EBfReQQFexW69kgVXn", bounds=bounds)
    if file_paths is None:
        print("No file paths provided")
        return None

    bounds_gdf = utils.bounds_to_gdf(bounds)
    all_dfs = []

    for path in file_paths:
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
        print("No data loaded")
        return None