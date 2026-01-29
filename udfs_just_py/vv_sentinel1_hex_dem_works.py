@fused.udf#(cache_max_age="0s")
def udf(
    bounds: fused.types.Bounds,
    provider="MSPC",
    time_of_interest="2025-03-01/2025-05-23",
    res:int=13,
    res_offset:int=10
):  
    """
    This UDF returns Sentinel 2 NDVI of the passed bounding box (viewport if in Workbench, or {x}/{y}/{z} in HTTP endpoint)
    Data fetched from either AWS S3 or Microsoft Planterary Computer
    """
    import odc.stac
    import planetary_computer
    import pystac_client
    import duckdb
    import numpy as np
    from pystac.extensions.eo import EOExtension as eo
    from utils import aggregate_df_hex, get_over,run_query

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
        collections=["sentinel-1-rtc"],
        bbox=bounds,
        datetime=time_of_interest,
        # query={"eo:cloud_cover": {"lt": 10}},
    ).item_collection()
    
    # Capping resolution to min 10m, the native Sentinel 2 pixel size
    # resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    # res_offset = -4 # lower makes the hex finer
    resolution = max(min(int(3 + zoom / 1.5), 12) - res_offset, 2)
    print(f"{resolution=}")

    if len(items) < 1:
        print(f'No items found. Please either zoom out or move to a different area')
    else:
        print(f"Returned {len(items)} Items")
    
        ds = odc.stac.load(
                items,
                crs="EPSG:3857",
                bands=['vv'],
                resolution=resolution,
                bbox=bounds,
            ).astype(float)
        print(ds)
        # ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])
        # Basic SAR indices
        vv = ds['vv']
        print(f"VV min: {vv.min().values}, max: {vv.max().values}")
        print(f"VV mean: {vv.mean().values}")
        # vh = ds['vh']
        # arr = vv.isel(time=-1) / vv.isel(time=0)
        # Convert DN to linear backscatter (typical Sentinel-1 scaling)
        vv_linear = 10**(vv / 10.0)  # Convert from dB-scaled DN to linear
        
        # OR if that doesn't work, try:
        # vv_linear = vv / 100.0  # Simple scaling
        
        # Then calculate std on the converted values  
        # arr = vv_linear.std(dim='time')
        vv_clean = vv.where((vv > -50) & (vv < 50))  # Keep reasonable dB range
        vv_clean = vv_clean.where(vv_clean != -32768)  # Remove NoData values
        
        # Calculate std on cleaned data
        arr = vv_clean.std(dim='time')
        
        # Remove any remaining NaNs/infs for H3 processing
        arr = arr.where(np.isfinite(arr))
        # # Radar Vegetation Index (RVI) - SAR equivalent of NDVI
        # rvi = (4 * vh) / (vv + vh)
        
        # # VH/VV ratio (good for vegetation/structure detection)
        # vh_vv_ratio = vh / vv
        
        # # Cross-pol ratio in dB
        # vh_vv_db = 10 * np.log10(vh) - 10 * np.log10(vv)
        
        # # Dual-pol index
        # dual_pol = (vv + vh) / 2
        
        # # Temporal aggregations
        # vv_max = vv.max(dim="time")
        # vh_max = vh.max(dim="time")
        # rvi_mean = rvi.mean(dim="time")
        # rvi_std = rvi.std(dim="time")
        # vh_vv_ratio_mean = vh_vv_ratio.mean(dim="time")
        
        # # Change detection between first and last dates
        # if len(ds.time) > 1:
        #    vv_change = vv.isel(time=-1) / vv.isel(time=0)
        #    vh_change = vh.isel(time=-1) / vh.isel(time=0)
        #    change_mask = (vv_change > 1.5) | (vv_change < 0.67) | (vh_change > 1.5) | (vh_change < 0.67)

        # print(arr)
        
        # print(ndvi.shape)
        # arr = ndvi.max(dim="time")
        # Use data from the most recent time.
         # arr = ds[band].max(dim="time")
        # if arr is None:
        #     return
        utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
        # bounds = utils.bounds_to_gdf(bounds)
        # bounds_values = bounds.bounds.values[0]
        df = utils.arr_to_latlng(arr.values, bounds)
        # print(df)
        df = aggregate_df_hex(df=df,stats_type='mean', res=res)
        # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) # hexify
        # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) #hexify
        # df_dem = fused.run("fsh_2KKOTd6HSiGtNOYHLqG4xN", bounds=tile, res=res) # USGS dem_10meter_tile_hex_2
        # df = duckdb.sql(" select df.*, df_dem.metric as elevation from df left join df_dem on df.hex = df_dem.hex").df()
        # df_roads = get_over(tile, overture_type="land_use")
        # bounds = utils.bounds_to_gdf(bounds)
        # bounds = bounds.bounds.values[0]
        # df = run_query(df_roads, df_sentinel, res, bounds)
        # df['elevation'] = df["elevation"] - 700
        # print(df)
        df = df[df['metric']>=1]
        print(df)
        return df
        # rgb_image = common_utils.visualize(
        #     data=arr,
        #     min=0,
        #     max=1,
        #     colormap=['black', 'green']
        # )
        # return rgb_image
