# a. mainstem
wbt_args = {
    "BreachDepressions": ["-i=dem.tif", "--fill_pits", "-o=dem_corr.tif"],
    "D8Pointer": ["-i=dem_corr.tif", "-o=fdir.tif"],
    "D8FlowAccumulation": ["-i=fdir.tif", "--pntr", "-o=d8accum.tif"],
    "FindMainStem": ["--d8_pntr=fdir.tif", "--streams=d8accum.tif", "-o=output.tif"],
}
min_max = None

# b. TWI
wbt_args = {
    "BreachDepressions": ["-i=dem.tif", "--fill_pits", "-o=dem_corr.tif"],
    "D8FlowAccumulation": ["-i=dem_corr.tif","--out_type='specific contributing area'","-o=sca.tif",],
    "Slope": ["-i=dem_corr.tif", "--units=degrees", "-o=slope.tif"],
    "WetnessIndex": ["--sca=sca.tif", "--slope=slope.tif", f"-o=output.tif"],
}
min_max = (0, 15)

@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds=None, 
        out_tif_name: str ='output', 
        wbt_args: dict = wbt_args, 
        min_max=min_max, 
        res:int=12
       ):
    import wbt
    import duckdb
    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)

    wbt_args = json.loads(wbt_args) if isinstance(wbt_args, str) else wbt_args
    df_flow = run(tile, wbt_args, out_tif_name, extra_input_files=None, min_max=min_max, res=res)
    # arr = arr.sqeeze()
    # bounds = common_utils.bounds_to_gdf(bounds)
    # bounds = bounds.bounds.values[0]
    # df = common_utils.arr_to_latlng(arr, bounds)
    # df = df[df["metric"] > 0]
    # df['metric']
    # print(df['metric'].describe())
    df_dem = fused.run("fsh_5BZM51QPfWM6BCpTAmhXxB", bounds=bounds, res=res, offset=10) # USGS dem_10meter_tile_hex_2
    # print(df_dem['hex'])
    # df_dem = fused.run("fsh_65CrKEyQM7ePE0X7PtzKBR", bounds=bounds, res=h3_size) # USGS
    # df = duckdb.sql("select df.*, df_dem.metric as elevation from df left join df_dem on df.hex = df_dem.hex").df()
    df = duckdb.sql(""" 
    select f.hex, 
e.metric as elev, 
    f.metric as flow,
   -- e.metric + f.metric as e_f
    e.metric * 10 - (LOG10(ABS(f.metric) + 1) * 55) as e_f
 -- (e.metric -1400) - (1 + LOG10((greatest(f.metric)) + 1) * 0.9) as e_f,
--e.metric - LOG10(abs(f.metric + 1)) * 100 as e_f
  --(e.metric - 1850) * (1 + LOG10(GREATEST(least(f.metric + 1, 1))) * 0.094) as e_f 
    from df_dem e inner join df_flow f 
    on e.hex=f.hex""").df()
    print(df.describe())
    return df

def df_to_hex(df, res, latlng_cols=("lat", "lng")):
    # xmin, ymin, xmax, ymax = bounds
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            array_agg(data+20) as agg_data
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
    df["metric"] = df.agg_data.map(fn)
    
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
    url = 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt'
    dem = rioxarray.open_rasterio(url).squeeze(drop=True)
    total_bounds = bounds.to_crs(5070).buffer(2000).to_crs(4326).total_bounds
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
