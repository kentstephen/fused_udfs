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
    # After stacking
    # After stacking
    signed_items = [planetary_computer.sign(item).to_dict() for item in items]
    ds = stackstac.stack(
        signed_items,
        # assets=[band],
        epsg=3857,
        bounds=bounds,
        # resolution=resolution,
    ).
    print(ds)
    # Debug the DataArray structure
    print(f"DataArray name: {ds.name}")
    print(f"DataArray dimensions: {ds.dims}")
    print(f"DataArray shape: {ds.shape}")
    
    # # Try to access the time dimension if it exists
    # if 'time' in ds.dims and ds.dims['time'] > 1:
    if len(ds) > 1:
        arr = ds["band"].max(dim="time")
    else:
        return
    
    if arr is None:
        return
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    # bounds = utils.bounds_to_gdf(bounds)
    # bounds_values = bounds.bounds.values[0]
    df = utils.arr_to_latlng(arr.values, bounds)
    print(df)
    df = aggregate_df_hex(df=df,stats_type='mean', res=res)
    
    # print(df)
    return df
        