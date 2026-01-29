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

@fused.udf
def udf(bounds: fused.types.Bounds=None, 
        out_tif_name: str ='output', 
        wbt_args: dict = wbt_args, 
        min_max=min_max, 
        res:int=11):
    import wbt
    import duckdb
    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)

    wbt_args = json.loads(wbt_args) if isinstance(wbt_args, str) else wbt_args
    df = wbt.run(tile, wbt_args, out_tif_name, extra_input_files=None, min_max=min_max, res=res)
    # arr = arr.sqeeze()
    # bounds = common_utils.bounds_to_gdf(bounds)
    # bounds = bounds.bounds.values[0]
    # df = common_utils.arr_to_latlng(arr, bounds)
    # df = df[df["metric"] > 0]
    # df['metric']
    # print(df['metric'].describe())
    df_dem = fused.run("fsh_2KKOTd6HSiGtNOYHLqG4xN", bounds=bounds, res=res) # USGS dem_10meter_tile_hex_2
    # print(df_dem['hex'])
    # df_dem = fused.run("fsh_65CrKEyQM7ePE0X7PtzKBR", bounds=bounds, res=h3_size) # USGS
    df = duckdb.sql("select df.*, df_dem.metric as elevation from df left join df_dem on df.hex = df_dem.hex").df()
    return df
