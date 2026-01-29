import json

# https://planetarycomputer.microsoft.com/dataset/modis-09A1-061
modis_params = json.dumps(
    {
        "collection": "modis-09A1-061",
        "band_list": [
            "sur_refl_b01",
            "sur_refl_b04",
            "sur_refl_b03",
            "sur_refl_b02",
        ],
        "time_of_interest": "2001-05/2001-07",
        "query": {
            "modis:horizontal-tile": {"gte": 0},  
        },
        "scale": 0.01,
    }
)

@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    collection_params=modis_params,
    chip_len: int = 256,
    scale: float = 0.1,
    show_ndvi = True,
    return_viz:bool=False
):
    import numpy as np   
    import geopandas as gpd  
 
    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    tile = common.get_tiles(bounds, clip=True)
    # Lad the geometry
    # # gdf = gpd.read_file('s3://fused-asset/data/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
    # if not chip_len:
    #     xmin,ymin,xmax,ymax = common.to_gdf(gdf, crs='UTM').total_bounds
    #     chip_len = int(max(xmax-xmin, ymax-ymin) / 10) # considering pixel size of 10m
    
    # Check to make sure the image is not too big
    print(f'{chip_len = }')
    if chip_len>3000:
        raise ValueError(f'THe image is too big ({chip_len=}>3000). Consider reducing your area of interest.')  

    # Get the data
    arr_rgbi = get_arr(tile, collection_params, output_shape=(chip_len, chip_len), nth_item=None)
    
    # Check if data was retrieved
    if arr_rgbi is None:
        print("No data retrieved for this area")
        return
    
    print(arr_rgbi.shape)
    
    # Scale the values for visualization purpose
    arr = rgbi_to_ndvi(arr_rgbi, return_viz)
    if arr is None:
        return
    
    return arr


@fused.cache
def get_arr(
    bounds,
    collection_params,
    output_shape,
    nth_item=None,
    max_items=150,

):
    
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    collection, band_list, time_of_interest, query, scale = json.loads(
        collection_params
    ).values()
    greenest_example = fused.load('https://github.com/fusedio/udfs/tree/a0af8a/community/sina/Satellite_Greenest_Pixel')

    stac_items = greenest_example.search_pc_catalog(
        bounds=bounds,
        time_of_interest=time_of_interest,
        query=query,
        collection=collection
    )
    if not stac_items: 
        print("No STAC items found")
        return None
        
    df_tiff_catalog = greenest_example.create_tiffs_catalog(stac_items, band_list)
    if len(df_tiff_catalog) > max_items:
        raise ValueError(f'{len(df_tiff_catalog)} > max number of images ({max_items})')  
    
    try:
        if nth_item:
            if nth_item > len(df_tiff_catalog):
                raise ValueError(f'{nth_item} > total number of images ({len(df_tiff_catalog)})') 
            df_tiff_catalog = df_tiff_catalog[nth_item:nth_item + 1]
            arrs_out = greenest_example.run_pool_tiffs(bounds, df_tiff_catalog, output_shape)
            arr = arrs_out.squeeze()
        else:
            arrs_out = greenest_example.run_pool_tiffs(bounds, df_tiff_catalog, output_shape)
            arr = greenest_example.get_greenest_pixel(arrs_out, how='max', fillna=True)
            arr = arr * 1.0 * scale
        return arr
    except ValueError as e:
        print(f"Error processing arrays (likely shape mismatch): {e}")
        print(f"Try reducing the number of images or checking data quality")
        return None
    
def rgbi_to_ndvi(arr_rgbi, return_viz): 
    import numpy as np
    import palettable
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    
    # MODIS surface reflectance scale factor is 0.0001
    # arr_rgbi = arr_rgbi * 0.0001
    
    # Handle NaN/invalid values and clip to valid range
    # arr_rgbi = np.clip(arr_rgbi, 0, 1)
    # arr_rgbi = np.nan_to_num(arr_rgbi, nan=0.0)
    
    # Calculate NDVI
    nir = arr_rgbi[-1].astype(float)  # B02 (NIR)
    red = arr_rgbi[0].astype(float)   # B01 (Red)
    
    denominator = nir + red
    ndvi = np.where(denominator != 0, (nir - red) / denominator, 0)
    
    # Clip to valid NDVI range
    ndvi = np.clip(ndvi, -1, 1)
    
    # Scale to uint16 for better precision
    # -1 maps to 0, 1 maps to 65535

    
    
    # ndvi = np.clip(ndvi, -1, 1)
    if return_viz:
        rgb_image = common.visualize(
            data=ndvi,
            min=0,    # Most vegetation NDVI is positive
            max=1,  # Typical healthy vegetation max
            colormap=palettable.matplotlib.Viridis_20,
        )
        return rgb_image
    else: 
        return ndvi