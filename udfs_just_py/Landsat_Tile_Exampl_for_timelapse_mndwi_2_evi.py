@fused.udf
def udf(
    bounds: fused.types.Bounds = [-101.412,35.659,-101.391,35.677],
    time_of_interest:str = "1985-04-01/1985-09-30",
     green_band="green",
    blue_band="blue",
    swir16_band="swir16",
    red_band="red",
    nir_band="nir08",
    collection="landsat-c2-l2",
    cloud_threshold=20,
):
    """Display NDVI based on Landsat & STAC"""
    import odc.stac
    import palettable
    import xarray
    import numpy as np
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo

    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    zoom = common.estimate_zoom(bounds)

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

    # Load imagery into an XArray dataset
    odc.stac.configure_s3_access(requester_pays=True)
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[red_band, nir_band],
        resolution=pixel_spacing,
        bbox=bounds,
    ).astype(float)
    # evi = 2.5 * ((ds[nir_band] - ds[red_band]) / 
    #          (ds[nir_band] + 6 * ds[red_band] - 7.5 * ds[blue_band] + 1))
    # Calculate the Normalized Difference Vegetation Index
    ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])
    # mndwi = (ds['green'] - ds['swir16']) / (ds['green'] + ds['swir16'])
    # Select the maximum value across all times
    arr = ndvi.max(dim="time")
    # return ndvi.max(dim="time")
    # ds = arr.to_dataset(name="ndvi")
    # return ds

    # # return arr.values
    return common.visualize(
        arr.values,
        min=0,
        max=0.5,
        colormap=palettable.matplotlib.Viridis_20,
    ), bounds
