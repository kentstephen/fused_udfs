@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection="3dep-lidar-dsm",
    band='data',
    res_factor:int=4,
    res:int=12
):
    import rioxarray
    import planetary_computer
    import pystac_client
    import warnings
    warnings.filterwarnings('ignore')
    
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)
    
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    
    search = catalog.search(
        collections=collection,
        bbox=bounds,
    )
    items = search.item_collection()
    print(f"Returned {len(items)} Items")
    
    if not items or len(items) == 0:
        return None
        
    resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    
    # Just take the first item since this runs per tile
    item = items[0]
    if band in item.assets:
        url = planetary_computer.sign(item.assets[band].href)
        
        # Open with rioxarray and process
        arr = rioxarray.open_rasterio(url).squeeze(drop=True)
        arr_reprojected = arr.rio.reproject("EPSG:3857", resolution=resolution)
        bounds_gdf = utils.bounds_to_gdf(bounds).to_crs("EPSG:3857")
        bounds = bounds_gdf.bounds.values[0]
        # Clip to bounds
        minx, miny, maxx, maxy = bounds
        arr_clipped = arr_reprojected.rio.clip_box(minx, miny, maxx, maxy)
        
        return arr_clipped.values.astype("uint8")
    
    return None