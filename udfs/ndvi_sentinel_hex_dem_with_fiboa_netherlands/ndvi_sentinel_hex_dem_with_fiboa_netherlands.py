@fused.udf#(cache_max_age="0s")
def udf(
    bounds: fused.types.Bounds,
    provider="AWS",
    time_of_interest="2020-07-15/2020-09-05",
    res:int=11,
    res_factor:int=7
):  
    """
    This UDF returns Sentinel 2 NDVI of the passed bounding box (viewport if in Workbench, or {x}/{y}/{z} in HTTP endpoint)
    Data fetched from either AWS S3 or Microsoft Planterary Computer
    """
    import odc.stac
    import planetary_computer
    import pystac_client
    import duckdb
    from pystac.extensions.eo import EOExtension as eo
    from utils import aggregate_df_hex, get_over,run_query, get_fields

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)
    zoom = tile.iloc[0].z
    
    if provider == "AWS":
        red_band = "red"
        nir_band = "nir"
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif provider == "MSPC":
        red_band = "B04"
        nir_band = "B08"
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
    else:
        raise ValueError(
            f'{provider=} does not exist. provider options are "AWS" and "MSPC"'
        )
    
    items = catalog.search(
        collections=["sentinel-2-l2a"],
        bbox=bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": 10}},
    ).item_collection()
    
    # Capping resolution to min 10m, the native Sentinel 2 pixel size
    resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    print(f"{resolution=}")

    if len(items) < 1:
        print(f'No items found. Please either zoom out or move to a different area')
    else:
        print(f"Returned {len(items)} Items")
    
        ds = odc.stac.load(
                items,
                crs="EPSG:3857",
                bands=[nir_band, red_band],
                resolution=resolution,
                bbox=bounds,
            ).astype(float)

        ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])
        ndvi_max = ndvi.max(dim='time')
        arr = ndvi_max
        # Use data from the most recent time.
         # arr = ds[band].max(dim="time")
        if arr is None:
            return
        utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
        # bounds = utils.bounds_to_gdf(bounds)
        # bounds_values = bounds.bounds.values[0]
        df = utils.arr_to_latlng(arr.values, bounds)
        
        df_sentinel = aggregate_df_hex(df=df,stats_type='mean', res=res)

        df_fiboa = get_fields(bounds, res)
        if len(df_fiboa)<1:
            return
        print(df_fiboa.columns)
        if df_fiboa is None or len(df_fiboa) == 0 or df_fiboa.shape[0] == 0:
            return
        if df_sentinel is None or len(df_sentinel) ==0 or df_sentinel.empty:
            return
        # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) # hexify
        # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) #hexify
        # df_dem = fused.run("fsh_2KKOTd6HSiGtNOYHLqG4xN", bounds=tile, res=res) # USGS dem_10meter_tile_hex_2
        # df = duckdb.sql(" select df.*, df_dem.metric as elevation from df left join df_dem on df.hex = df_dem.hex").df()
        # df_roads = get_over(tile, overture_type="land_use")
        # bounds = utils.bounds_to_gdf(bounds)
        # bounds = bounds.bounds.values[0]
        df = run_query(df_fiboa, df_sentinel, res, bounds)
        # df['elevation'] = df["elevation"] - 700
        # print(df)
        return df
        # rgb_image = common_utils.visualize(
        #     data=arr,
        #     min=0,
        #     max=1,
        #     colormap=['black', 'green']
        # )
        # return rgb_image
