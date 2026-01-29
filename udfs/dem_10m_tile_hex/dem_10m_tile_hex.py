# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection="3dep-seamless",
    band="data",
    res_factor:int=4,
    res:int=11
):

    import odc.stac
    
    import palettable
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    from utils import aggregate_df_hex, df_to_hex

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
    # print(items[0].assets.keys()) 
    # print(f"Returned {len(items)} Items")
    resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    print(f"{resolution=}")
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
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    # bounds = utils.bounds_to_gdf(bounds)
    # bounds_values = bounds.bounds.values[0]
    df = utils.arr_to_latlng(arr.values, bounds)
    
    df = aggregate_df_hex(df=df,stats_type='mean', res=res)
    # df['metric'] = df["metric"] - 1000
    # print(df)
    return df

    # rgb_image = utils.visualize(
    #     data=arr,
    #     min=0,
    #     max=100,
    #     colormap=palettable.matplotlib.Viridis_20,
    # )
    # return rgb_image