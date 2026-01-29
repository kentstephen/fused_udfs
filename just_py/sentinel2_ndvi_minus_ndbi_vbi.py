@fused.udf#(cache_max_age="0s")
def udf(
    bounds: fused.types.Bounds,
    provider="AWS",
        time_of_interest="2024-07-15/2024-08-20",
    res:int =12,
    res_factor:int=5
):  

    import odc.stac
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    import utils
    from utils import aggregate_df_hex

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)
    zoom = common_utils.estimate_zoom(bounds)
    if provider == "AWS":
        green_band = "green"  # Add this line
        red_band = "red"
        nir_band = "nir"
        swir_band = "swir16"
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif provider == "MSPC":
        green_band = "B03"  # Add this line
        red_band = "B04"
        nir_band = "B08"
        swir_band = "B12"
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
                bands=[nir_band, swir_band, red_band, green_band],
                resolution=resolution,
                bbox=bounds,
            ).astype(float)
        # ndbi (SWIR - NIR)/(SWIR + NIR) 
        # 
        ndbi = (ds[swir_band] - ds[nir_band]) / (ds[swir_band] + ds[nir_band])
        # mndwi = (ds[green_band] - ds[nir_band]) / (ds[green_band] + ds[nir_band]) # water
        ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])
        vbi = ndvi - ndbi
        print(vbi.shape)
        arr = vbi.max(dim="time")
        if arr is None:
            return
        utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
        # bounds = utils.bounds_to_gdf(bounds)
        # bounds_values = bounds.bounds.values[0]
        df = utils.arr_to_latlng(arr.values, bounds)
        # gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=bounds, overture_type="land")
        # print(df)
        # return df
        df = aggregate_df_hex(bounds, df=df,stats_type='mean', res=res)
        # df['metric'] = df['metric'] -15
        # df = df[df['metric']<0]
        print(df)
        return df
        # rgb_image = common_utils.visualize(
        #     data=arr,
        #     min=0,
        #     max=1,
        #     colormap=['black', 'green']
        # )
        # return rgb_image
