@fused.udf
def udf(
    bounds: fused.types.Bounds = [-101.412,35.659,-101.391,35.677],
    time_of_interest="1994-05-01/1995-09-30",
     green_band="green",
    blue_band="blue",
    swir16_band="swir16",
    red_band="red",
    nir_band="nir08",
    collection="landsat-c2-l2",
    cloud_threshold=10,
    evi:bool=False
):
    """Display NDVI based on Landsat & STAC"""
    import odc.stac
    import palettable
    import rioxarray as rio
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
    if evi: 
        ds = odc.stac.load(
            items,
            crs="EPSG:3857",
            bands=[red_band, nir_band, blue_band],
            resolution=pixel_spacing,
            bbox=bounds,
        ).astype(float)
        if not ds.data_vars:
            return pd.DataFrame({})
        
        # Apply Landsat C2 L2 scaling
        ds = ds * 0.0000275 - 0.2
        
        evi = 2.5 * ((ds[nir_band] - ds[red_band]) / 
                    (ds[nir_band] + 6 * ds[red_band] - 7.5 * ds[blue_band] + 1))
        evi = evi.clip(min=-1.0, max=1.0) 
        arr = evi.max(dim="time")
    else:
        ds = odc.stac.load(
            items,
            crs="EPSG:3857",
            bands=[red_band, nir_band],
            resolution=pixel_spacing,
            bbox=bounds,
        ).astype(float)
        if not ds.data_vars:
            return pd.DataFrame({})
        ds = ds * 0.0000275 - 0.2
        ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])
        ndvi = ndvi.clip(min=-1.0, max=1.0) 
        arr = ndvi.max(dim="time")
    
    # return common.visualize(
    #     arr.values,
    #     min=-1,
    #     max=1,
    #     colormap=palettable.scientific.sequential.Bamako_20_r,
    # ), bounds

    # Calculate the Normalized Difference Vegetation Index

    da_tiff = arr.rio.write_crs("EPSG:3857").rio.reproject("EPSG:4326")
    df_latlng = da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]
    return df_latlng
