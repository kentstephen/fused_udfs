@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection="3dep-seamless",
    band="data",
    res_factor:int=4, # higher means higher res
    h3_size:int= 11
):

    import odc.stac
    import palettable
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    from utils import aggregate_df_hex

    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    items = catalog.search( 
        collections=[collection],
        bbox=bounds,
    ).item_collection()
    if not items or len(items) == 0:
        print("No items found for the given bounds and collection")
        return
    resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    print(f"raster {resolution=}")
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[band],
        resolution=resolution,
        bbox=bounds,
    ).astype(float)
    
    # Use data from the most recent time.
    arr = ds[band].max(dim="time")
    if arr is None:
        return
   
    # Create lat/lng/value for 
    df = utils.arr_to_latlng(arr.values, bounds)
    # Transform the bounds for these to functions
    # bounds_gdf = utils.bounds_to_gdf(bounds)
    # bounds_values = bounds_gdf.bounds.values[0]
    df = aggregate_df_hex(df=df,stats_type='mean', res=h3_size)
    
    # print(df)
    return df

    # Comment out the Pandas code to see the original visualization
    # rgb_image = utils.visualize(
    #     data=arr,
    #     min=0,
    #     max=100,
    #     colormap=palettable.matplotlib.Viridis_20,
    # )
    # return rgb_image