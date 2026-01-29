@fused.udf
def udf(
    bounds: fused.types.Bounds,
    provider="MSPC",
    time_of_interest="2025-03-15/2025-06-15",
    res:int = 8
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
    from utils import aggregate_df_hex
    import numpy as np
    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)
    zoom = common_utils.estimate_zoom(bounds)
    if provider == "AWS":
        green_band = "green"  # Add this line
        red_band = "red"
        nir_band = "nir"
        swir_band = "swir16"
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif provider == "MSPC":
        # green_band = "B03"  # Add this line
        # red_band = "B04"
        # nir_band = "B08"
        # swir_band = "B12"
        # nir_band = "B05"
        # swir_band = "B06"
        nir_band = "B05"   # Keep NIR
        water_band = "B03" 
        
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
    else:
        raise ValueError(
            f'{provider=} does not exist. provider options are "AWS" and "MSPC"'
        )
    
    items = catalog.search(
        collections=["hls2-l30"],
        bbox=bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": 10}},
    ).item_collection()
    
    # Capping resolution to min 10m, the native Sentinel 2 pixel size
    resolution = int(10 * 2 ** max(0, (15 - zoom)))
    print(f"{resolution=}")

    if len(items) < 1:
        print(f'No items found. Please either zoom out or move to a different area')
    else:
        print(f"Returned {len(items)} Items")
    
        ds = odc.stac.load(
                items,
                crs="EPSG:3857",
                bands=[nir_band,water_band],# red_band, green_band],
                resolution=resolution,
                bbox=bounds,
            ).astype(float)

        # 
        # ndbi = (ds[swir_band] - ds[nir_band]) / (ds[swir_band] + ds[nir_band])
        # mndwi = (ds[green_band] - ds[nir_band]) / (ds[green_band] + ds[nir_band])
            # One-line version of the NBUI formula
        # nbui = ((ds[swir_band] - ds[nir_band])/(10.0 * (T + ds[swir_band])**0.5)) - \
        #        (((ds[nir_band] - ds[red_band]) * (1.0 + L))/(ds[nir_band] - ds[red_band] + L)) - \
        #        (ds[green_band] - ds[swir_band])/(ds[green_band] + ds[swir_band])
        # print(bui.shape)
        # Change your bands:
        # nir_band = "B05"   # Keep NIR
        # water_band = "B03"  # Use GREEN instead of SWIR
    
        # Calculate NDWI
        green = ds[water_band]
        nir = ds[nir_band]
        
        ndwi = (green - nir) / (green + nir + 1e-10)
        ndwi = ndwi.clip(-1, 1)
        arr = ndwi.median(dim="time")
        # ndmi = (ds[nir_band] - ds[swir_band]) / (ds[nir_band] + ds[swir_band] + 1e-10)
        # arr = ndmi.median(dim="time")
        if arr is None:
            return
        arr = np.where(arr > 100, np.nan, arr)  # Mask anything absurdly high
        arr = np.where(arr < -100, np.nan, arr)  # Mask anything absurdly low
        utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
        # bounds = utils.bounds_to_gdf(bounds)
        # bounds_values = bounds.bounds.values[0]
        df = arr_to_latlng(arr, bounds)
        # gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=bounds, overture_type="land")
        
        df = aggregate_df_hex(bounds, df=df,stats_type='mean', res=res)
        
        print(df)
        return df
        # rgb_image = common_utils.visualize(
        #     data=arr,
        #     min=0,
        #     max=1,
        #     colormap=['black', 'green']
        # )
        # return rgb_image
def arr_to_latlng(arr, bounds):
    import numpy as np
    import pandas as pd
    from pyproj import Transformer
    
    # Get dimensions
    height, width = arr.shape[-2:]
    
    # Convert bounds from EPSG:4326 to EPSG:3857
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    min_x, min_y = transformer.transform(bounds[0], bounds[1])
    max_x, max_y = transformer.transform(bounds[2], bounds[3])
    
    # Calculate pixel size in 3857
    pixel_width = (max_x - min_x) / width
    pixel_height = (max_y - min_y) / height
    
    # Create coordinate arrays at pixel centers (EPSG:3857)
    x = np.linspace(min_x + pixel_width/2, max_x - pixel_width/2, width)
    y = np.linspace(min_y + pixel_height/2, max_y - pixel_height/2, height)
    X, Y = np.meshgrid(x, y)
    
    # Transform back to EPSG:4326
    transformer_back = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    lng, lat = transformer_back.transform(X.flatten(), Y.flatten())
    
    return pd.DataFrame({
        "lng": lng,
        "lat": lat,
        "data": arr.flatten()
    })