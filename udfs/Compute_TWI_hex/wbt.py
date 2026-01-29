def df_to_hex(df, res, latlng_cols=("lat", "lng")):
    # xmin, ymin, xmax, ymax = bounds
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            array_agg(data) as agg_data
            FROM df

            group by 1
          -- order by 1
        """
    con = utils.duckdb_connect()
    return con.query(qr).df()
    

    # return con.query(qr).df()
def aggregate_df_hex(df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import pandas as pd
    import numpy as np
    
    # Convert to hexagons
    df = df_to_hex(df, res=res, latlng_cols=latlng_cols)
    
    # Define aggregation functions that handle null values
    if stats_type == "sum":
        fn = lambda x: pd.Series(x).fillna(0).sum()
    # elif stats_type == "mean":
    #     fn = lambda x: np.maximum(0, np.array([val for val in x if val is not None], dtype=float)).mean()
    else:
        fn = lambda x: pd.Series(x).fillna(10).mean()
    
    # Apply the aggregation function to create the metric column
    df["twi_metric"] = df.agg_data.map(fn)
    
    # Replace any remaining NaN values with 0
    # df["metric"] = df["metric"].fillna(0)
    
    return df
def run(bounds, wbt_args: dict[str, list[str]], out_tif_name: str, extra_input_files: list[str] | None = None, min_max: tuple[float, float] | None = None, res: int=None):
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils
    import tempfile
    import rioxarray
    import numpy as np
    import pywbt
    import shutil

    extra_input_files = [] if extra_input_files is None else extra_input_files
    url = 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1/TIFF/USGS_Seamless_DEM_1.vrt'
    dem = rioxarray.open_rasterio(url).squeeze(drop=True)
    total_bounds = bounds.to_crs(5070).buffer(1000).to_crs(4326).total_bounds
    dem = dem.rio.clip_box(*total_bounds)
    dem = dem.where(dem > dem.rio.nodata, drop=False).rio.write_nodata(np.nan)

    crs_proj = 5070
    bbox_proj = bounds.to_crs(crs_proj).total_bounds
    dem = dem.rio.reproject(crs_proj).rio.clip_box(*bbox_proj)
    
    with tempfile.TemporaryDirectory() as tmp:
        dem.rio.to_raster(f"{tmp}/dem.tif")
        for f in extra_input_files:
            shutil.copy(f, tmp)
        pywbt.whitebox_tools(tmp, wbt_args, save_dir=tmp, wbt_root="WBT", zip_path="wbt_binaries.zip")
        ds = rioxarray.open_rasterio(f"{tmp}/{out_tif_name}.tif").squeeze(drop=True)
        ds = ds.where(ds > ds.rio.nodata)
        arr = ds.rio.reproject(bounds.crs).rio.clip_box(*bounds.total_bounds).to_numpy()
        common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
        # bounds = common_utils.bounds_to_gdf(bounds)
        bounds = bounds.bounds.values[0]
        df = common_utils.arr_to_latlng(arr, bounds)
        dem_hex_utils = fused.load("https://github.com/fusedio/udfs/tree/0b1bd10/public/DEM_Tile_Hexify/").utils
        df = aggregate_df_hex(df, res)
        
    # return utils.arr_to_plasma(arr, min_max=min_max, reverse=False)
    print(df)
    return df
