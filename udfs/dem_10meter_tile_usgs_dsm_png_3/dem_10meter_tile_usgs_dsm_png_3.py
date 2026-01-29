@fused.udf
def udf(
    bounds: fused.types.Bounds=None,
    collection="3dep-lidar-dsm",
    band='data',
    res_factor: int = 4,
    res: int = 13
):
    import odc.stac
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
    
    # Get bounds consistently
    bounds_gdf = utils.bounds_to_gdf(bounds)
    minx, miny, maxx, maxy = bounds_gdf.bounds.values[0]
    
    search = catalog.search(
        collections=collection,
        bbox=[minx, miny, maxx, maxy]
    )
    
    items = search.item_collection()
    print(f"Returned {len(items)} Items")
    
    if not items or len(items) == 0:
        return None
    
    # Use stackstac with corrected bounds
    stack = stackstac.stack(
        items,
        assets=[band],
        bounds=(minx, miny, maxx, maxy),  # Use tuple instead of list
        epsg=4326,
        resolution=0.00005,  # Higher resolution
        fill_value=0
    )
    
    print(f"Stack shape: {stack.shape}")
   
    # Take first time slice and band
    arr = stack.isel(time=0, band=0).values
    
    # return arr.astype("uint8")
    print(f"Array shape: {arr.shape}")
    
    # Convert to dataframe for hex aggregation
    df = utils.arr_to_latlng(arr, bounds)
    print(df)
    # Aggregate to hex
    df = aggregate_df_hex(bounds_gdf.bounds.values[0], df=df, stats_type='mean', res=res)
    # df = df[df["metric"] > 0]
    # df.metric = df.metric - 1000
    
    return df