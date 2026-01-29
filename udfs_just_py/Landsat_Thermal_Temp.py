def udf(
    bbox,
    time_of_interest="2023-05-10/2023-07-01",
    thermal_band="lwir11",
    collection="landsat-c2-l2",
):
    import odc.stac
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    import numpy as np
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils
    print(bbox.z)
    # Configure S3 access for Landsat data
    odc.stac.configure_s3_access(requester_pays=True)
    
    # Open the STAC catalog
    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    
    # Print the search parameters for debugging
    print(f"Searching for items in collection '{collection}' within bbox {bbox.total_bounds} and date range '{time_of_interest}'")

    # Search for items in the specified collection, bounding box, and time range
    items = catalog.search(
        collections=[collection],
        bbox=bbox.total_bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": 20}},  # Increased cloud cover for debugging
    ).item_collection()

    print(f"Returned {len(items)} Items")
    if len(items) == 0:
        print("No items found. Please check the bounding box and date range.")
        return None

    # Calculate resolution
    resolution = int(5 * 2 ** (18 - bbox.z[0]))
    print(f"{resolution=}")

    # Load the thermal band
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[thermal_band],
        resolution=resolution,
        bbox=bbox.total_bounds,
    ).astype(float)

    # Convert digital numbers (DN) to radiance
    radiance = (ds[thermal_band] * 3.3420E-04) + 0.1

    # Convert radiance to surface temperature in Kelvin
    K1 = 774.89  # Calibration constant
    K2 = 1321.08  # Calibration constant
    temperature_kelvin = K2 / np.log((K1 / radiance) + 1)

    # Convert surface temperature from Kelvin to Celsius
    temperature_celsius = temperature_kelvin - 273.15

    # Create a visualization of the surface temperature
    arr = temperature_celsius.max(dim="time")
    return utils.arr_to_plasma(arr.values, min_max=(0, 100))  
