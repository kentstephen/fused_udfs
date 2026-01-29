@fused.udf
def udf(bounds: fused.types.Bounds = None):
    import tempfile
    import rioxarray
    import numpy as np
    import pywbt
    import pandas as pd
    
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    file_paths = fused.run("fsh_6PtF7mYSGIThFIB0SPdG8f",
                           bounds=bounds
                          )
    
    if file_paths is None:
        return None
    
    all_dfs = []
    
    for path in file_paths:
        try:
            with rioxarray.open_rasterio(path, chunks='auto') as da_tiff:
                raster_crs = da_tiff.rio.crs
                
                # Transform bounds to raster's CRS and buffer there
                bounds_gdf = common.bounds_to_gdf(bounds)
                bounds_gdf_native = bounds_gdf.to_crs(raster_crs)
                bounds_buffered = bounds_gdf_native.buffer(1000)
                
                total_bounds = bounds_buffered.total_bounds
                da_buffered = da_tiff.rio.clip_box(*total_bounds)
                
                with tempfile.TemporaryDirectory() as tmp:
                    da_buffered.rio.to_raster(f"{tmp}/dem.tif")
                    
                    wbt_args = {
                        "BreachDepressions": ["-i=dem.tif", "--fill_pits", "-o=dem_corr.tif"],
                        "D8FlowAccumulation": ["-i=dem_corr.tif", "-o=flow.tif"]
                    }
                    pywbt.whitebox_tools(tmp, wbt_args, save_dir=tmp, 
                                        wbt_root="WBT", zip_path="wbt_binaries.zip")
                    
                    flow = rioxarray.open_rasterio(f"{tmp}/flow.tif").squeeze(drop=True)
                    
                    # DON'T clip back - keep the buffered area
                    flow_4326 = flow.rio.reproject("EPSG:4326")
                    df = flow_4326.to_dataframe("flow").reset_index()[["y", "x", "flow"]]
                    df = df.dropna()
                    
                    if len(df) > 0:
                        all_dfs.append(df)
                        
        except Exception as e:
            print(f"Error: {e}")
            continue
    
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return None