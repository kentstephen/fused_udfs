@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection="3dep-lidar-dsm",
    band='data',
    res_factor: int = 4,
    res: int = 12
):
    import stackstac
    import planetary_computer
    import pystac_client
    import warnings
    warnings.filterwarnings('ignore')
    from utils import aggregate_df_hex
    
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)
    
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    
    # Get bounds as tuple
    bounds_gdf = utils.bounds_to_gdf(bounds)
    minx, miny, maxx, maxy = bounds_gdf.bounds.values[0]
    
    search = catalog.search(
        collections=collection,
        bbox=[minx, miny, maxx, maxy],
        datetime="2015",
    )
    items = search.item_collection()
    print(f"Returned {len(items)} Items")
    
    if not items or len(items) == 0:
        return None
    
    # Calculate resolution
    resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    
    # Use stackstac to create aligned raster stack
    stack = stackstac.stack(
        items,
        assets=[band],
        bounds=[minx, miny, maxx, maxy],
        epsg=4326,
        resolution=0.0001,  # Consistent resolution in degrees
        fill_value=0
    )
    
    # print(f"Stack shape: {stack.shape}")
    # print(f"Stack bounds: {stack.rio.bounds()}")
    
    # Take first time slice and band
    arr = stack.isel(time=0, band=0).values
    print(arr.shape)
    # return arr
    # Convert to dataframe for hex aggregation
    df = utils.arr_to_latlng(arr, bounds)
    print(df)
    # print(f"DataFrame shape: {df.shape}")
    
    # Aggregate to hex
    df = aggregate_df_hex(bounds_gdf.bounds.values[0], df=df, stats_type='mean', res=res)
    df = df[df["metric"] > 0]
    print(df)
    # print(f"Final hex DataFrame: {df.shape}")
    return df