@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    time_of_interest="1992-04-01/1992-12-30",
    red_band="red",
    green_band="green",
    blue_band="blue",
    nir_band="nir08",
    collection="landsat-c2-l2",
    cloud_threshold=5,
    scale:float=0.0001
):
    """Display NDVI based on Landsat & STAC"""
    import odc.stac
    import palettable
    import pystac_client
    import numpy as np
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
    # odc.stac.configure_s3_access(requester_pays=True)
    # ds = odc.stac.load(
    #     items,
    #     crs="EPSG:3857",
    #     bands=[red_band, green_band, blue_band],
    #     resolution=pixel_spacing,
    #     bbox=bounds,
    # ).astype(float)

    # # Calculate the Normalized Difference Vegetation Index
    # ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])

    # # Select the maximum value across all times
    odc.stac.configure_s3_access(requester_pays=True)
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[red_band, green_band, blue_band],
        resolution=pixel_spacing,
        bbox=bounds,
    ).astype(float)
    
    # Extract RGB bands
    # rgb = np.stack([ds[red_band], ds[green_band], ds[blue_band]])
    arr_out = np.stack([ds[red_band].max(dim="time"), 
                        ds[green_band].max(dim="time"), 
                        ds[blue_band].max(dim="time")])
    
    # Apply Landsat Collection 2 surface reflectance scaling
    arr_scaled = arr_out * 1.0 * 0.000055
    
    # Convert to 0-255 range for display
    arr_scaled = np.clip(arr_scaled * 255, 0, 255).astype("uint8")
    
    return arr_scaled, bounds
    # return common.visualize(
    #     arr.values,
    #     min=0,
    #     max=0.5,
    #     colormap=palettable.scientific.sequential.Bamako_20_r,
    # ), bounds
