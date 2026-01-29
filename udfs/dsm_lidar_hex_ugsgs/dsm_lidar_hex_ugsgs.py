# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection="3dep-lidar-dsm",
    band="data",
    # res_factor:int=4,
    res:int= 9,
    time_range = "2000/2023",
    res_factor:int=4,
):
    import stackstac
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
    # Get items as a proper list instead of ItemCollection
    items = catalog.search(
        collections=[collection],
        bbox=bounds,
        datetime=time_range
    ).item_collection()
    
    print(f"Type of items: {type(items)}")
    print(f"Number of items: {len(items)}")
    print(f"First item type: {type(items[0]) if items else 'No items'}")
    resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    ds = stackstac.stack(
        items,
        assets=[band],
        epsg=3857,  # Web Mercator
        bounds=bounds,
        resolution=resolution,  # Optional: specify resolution in meters
    ).squeeze()  
    
    # Use data from the most recent time.
    arr = ds[band].max(dim="time")
    if arr is None:
        return
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    # bounds = utils.bounds_to_gdf(bounds)
    # bounds_values = bounds.bounds.values[0]
    df = utils.arr_to_latlng(arr.values, bounds)
    
    df = aggregate_df_hex(df=df,stats_type='mean', res=res)
    
    # print(df)
    return df

    # rgb_image = utils.visualize(
    #     data=arr,
    #     min=0,
    #     max=100,
    #     colormap=palettable.matplotlib.Viridis_20,
    # )
    # return rgb_image