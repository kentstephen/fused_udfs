@fused.udf#(cache_max_age="0s")
def udf(
    bounds: fused.types.Bounds,
    provider="AWS",
         time_of_interest="2023-06-25/2023-09-10",
    res:int=9,
    res_factor:int=30,
    tree_scale: int = 200,
    show_dem: bool= False
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
    import pandas as pd
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
        collections=["sentinel-2-l2a"],
        bbox=bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": 2}},
    ).item_collection()
    
    # Capping resolution to min 10m, the native Sentinel 2 pixel size
    # resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    res_offset = -4 # lower makes the hex finer
    resolution = max(min(int(3 + zoom / 1.5), 12) - res_offset, 2)
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
        print(ndvi.shape)
        arr = ndvi.max(dim="time")
        print(arr)
        # Use data from the most recent time.
         # arr = ds[band].max(dim="time")
        if arr is None:
            return
        utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
   
        df = utils.arr_to_latlng(arr.values, bounds)
        # lons, lats = np.meshgrid(arr.longitude.values, arr.latitude.values)
        # df = pd.DataFrame({
        #     'lng': lons.flatten(),
        #     'lat': lats.flatten(), 
        #     'data': arr.values.flatten()
        #    })
        # df = df.dropna() 
        # print(f"first transformation{df}")
        bounds_gdf = utils.bounds_to_gdf(bounds)
        bounds_values = bounds_gdf.bounds.values[0]
        df = aggregate_df_hex(bounds_values=bounds_values, df=df,stats_type='mean', res=res,)
        df_dem = fused.run("fsh_3ailz7Y3EAoBnz7G4pPYPJ", bounds=bounds, res=res, tree_scale=tree_scale, show_dem=show_dem) # meta trees
        # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) # hexify
        # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) #hexify
        # df_dem = fused.run("fsh_2KKOTd6HSiGtNOYHLqG4xN", bounds=tile, res=res) # USGS dem_10meter_tile_hex_2
        df = duckdb.sql(" select df.hex, coalesce(df.metric,5.01) as ndvi, df_dem.total_elevation, df_dem.canopy_height from df left join df_dem on df.hex = df_dem.hex-- where canopy_height >0").df()
        # df_roads = get_over(tile, overture_type="land_use")
        # bounds = utils.bounds_to_gdf(bounds)
        # bounds = bounds.bounds.values[0]
        # df = run_query(df_roads, df_sentinel, res, bounds)
        # df['elevation'] = df["elevation"] - 2000
        # print(df)
        df['ndvi'] = df['ndvi']-5
        return df
        # rgb_image = common_utils.visualize(
        #     data=arr,
        #     min=0,
        #     max=1,
        #     colormap=['black', 'green']
        # )
        # return rgb_image
