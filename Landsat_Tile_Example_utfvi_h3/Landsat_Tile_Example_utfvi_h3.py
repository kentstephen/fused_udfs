@fused.udf
def udf(
    bounds: fused.types.Bounds,
    time_of_interest="2024-06-01/2024-08-30",
    red_band="red",
    nir_band="nir08",
    thermal_band="lwir11",
    collection="landsat-c2-l2",
    cloud_threshold=10,
    res:int = 10,
):
    """Calculate and display UTFVI based on Landsat & STAC"""
    import odc.stac
    import palettable
    import pystac_client
    import numpy as np
    from pystac.extensions.eo import EOExtension as eo
    from utils import aggregate_df_hex
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)
    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    
    # Search for imagery within a specified bounding box and time period
    items = catalog.search(
        collections=[collection],
        bbox=bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": cloud_threshold}},
    ).item_collection()
    print(f"Returned {len(items)} Items")
    
    # Determine the pixel spacing for the zoom level
    pixel_spacing = int(5 * 2 ** (15 - zoom))
    print(f"{pixel_spacing = }")
    
    # Load imagery into an XArray dataset - include all needed bands
    odc.stac.configure_s3_access(requester_pays=True)
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[red_band, nir_band, thermal_band],  # Load all needed bands
        resolution=pixel_spacing,
        bbox=bounds,
    ).astype(float)
    
    lst = ds[thermal_band] 
    if 'time' in lst.dims and lst.sizes['time'] > 1:
        lst = lst.median(dim='time', skipna=True)
    
    arr = lst.where(lst > 250) /1000
    # print(ds)
    # Handle any NaN values for visualization
    # utfvi_filled = utfvi.fillna(1.0)  # Fill with neutral value (1.0 = mean temperature)
    
    # print(f"LST mean: {lst_mean.values:.2f}K")
    # print(f"UTFVI range: {utfvi_filled.min().values:.4f} to {utfvi_filled.max().values:.4f}")
    # print(f"UTFVI shape: {utfvi_filled.shape}")
    print(arr)
    if arr is None:
        return
    # utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    # # bounds = utils.bounds_to_gdf(bounds)
    # # bounds_values = bounds.bounds.values[0]
    # df = utils.arr_to_latlng(arr.values, bounds)
    # bounds = utils.bounds_to_gdf(bounds)
    # bounds = bounds.bounds.values[0]
    # df = aggregate_df_hex(df=df, bounds=bounds,stats_type='mean', res=res)
    # # df['metric'] = df["metric"] - 1000
    # print(df)
    # return df
    # Visualize with appropriate range for UTFVI
    return utils.visualize(
        arr.values,
        min=44,  # Slightly below mean (cooler areas)
        max=55,  # Slightly above mean (warmer areas)
        colormap=palettable.cartocolors.diverging.Geyser_7,
    )