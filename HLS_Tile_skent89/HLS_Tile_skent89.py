# To Get your username and password, Please visit https://urs.earthdata.nasa.gov
@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    collection_id="HLSL30.v2.0",  # Landsat:'HLSL30.v2.0' & Sentinel:'HLSS30.v2.0'
    band="B10",  # Landsat:'B05' & Sentinel:'B8A'
    date_range="2016-01/2023-12",
    max_cloud_cover=25,
    n_mosaic=5,
    min_max=(0, 3000),
    username="skent89",
    password="W&70Kv&&O$prtSk",
    env="earthdata",
):
    z = bbox.z[0]
    if z >= 9:
        utils = fused.load(
            "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
        ).utils
        import numpy as np
        import rasterio
        from rasterio.transform import from_origin
        import numpy as np
        from pystac_client import Client
        from utils import raster_to_h3_indices, h3_to_geodataframe, get_transform_from_bbox
        from rasterio.transform import from_bounds
        STAC_URL = "https://cmr.earthdata.nasa.gov/stac"
        catalog = Client.open(f"{STAC_URL}/LPCLOUD/")
        search = catalog.search(
            collections=[collection_id],
            bbox=bbox.total_bounds,
            datetime=date_range,
            limit=100,
        )
        item_collection = search.get_all_items()
        band_urls = []
        for i in item_collection:
            if (
                i.properties["eo:cloud_cover"] <= max_cloud_cover
                and i.collection_id == collection_id
                and band in i.assets
            ):
                url = i.assets[band].href
                url = url.replace(
                    "https://data.lpdaac.earthdatacloud.nasa.gov/", "s3://"
                )
                band_urls.append(url)
        if len(band_urls) == 0:
            print(
                "No items were found. Please check your `collection_id` and `band` and widen your `date_range`."
            )
            return None
        try:
            aws_session = utils.earth_session(
                cred={"env": env, "username": username, "password": password}
            )
        except:
            print(
                "Please set `username` and `password` arguments. Create credentials at: https://urs.earthdata.nasa.gov to register and manage your Earthdata Login account."
            )
            return None
        cred = {"env": env, "username": username, "password": password}
        mosaic_reduce_function = lambda x: np.max(x, axis=0)
        arr = utils.mosaic_tiff(
            bbox,
            band_urls[:n_mosaic],
            mosaic_reduce_function,
            overview_level=max(0, 12 - z),
            cred=cred,
        )
        output = utils.arr_to_plasma(arr, min_max)
        print(f"{i.assets.keys()=}")
        print(f"{len(band_urls)=}")
        print(f"{arr.min()=}")
        print(f"{arr.max()=}")
        
        print(output)
        raster_shape = output.shape[1:]
        transform = get_transform_from_bbox(bbox, raster_shape)
        # return output
        # Set the H3 resolution
        resolution = 9
        
        # Convert raster data to H3 indices
        h3_aggregated = raster_to_h3_indices(output, transform, resolution)
        # Convert H3 data to GeoDataFrame
        gdf = h3_to_geodataframe(h3_aggregated)
        return gdf
        print(gdf)


    elif z >= 8:
        print("Almost there! Please zoom in a bit more. 😀")
    else:
        print("Please zoom in more.")
