@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    provider="MSPC",
    time_of_interest="2023-01-01/2024-12-30"
):  
    """
    This UDF returns Sentinel 2 NDVI of the passed bounding box (viewport if in Workbench, or {x}/{y}/{z} in HTTP endpoint)
    Data fetched from either AWS S3 or Microsoft Planterary Computer
    """
    import odc.stac
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    import utils
    import numpy as np
    
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
            collections=["sentinel-1-grd"],
            bbox=bbox.total_bounds,
            datetime=time_of_interest,
            query=None,
        ).item_collection()
    
    # Capping resolution to min 10m, the native Sentinel 2 pixel size
    resolution = int(10 * 2 ** max(0, (15 - bbox.z[0])))
    print(f"{resolution=}")

    if len(items) < 1:
        print(f'No items found. Please either zoom out or move to a different area')
    else:
        print(f"Returned {len(items)} Items")
    
    ds = odc.stac.load(
        items,
        crs="EPSG:4326",
        bands=['vv'],
        resolution=resolution,
        bbox=bbox.total_bounds,
    ).astype(float)

    arr = ds.vv.max(dim="time") # highlight high backscatter areas like buildings
    # # print(ds)
    # print("Min value:", ds.vv.min().values)
    # print("Max value:", ds.vv.max().values)
    # rgb_image = utils.visualize(
    #     data=arr,
    #     min=-56,
    #     max=2000,
    #     colormap=['black', 'green']
    # )
    return arr
