@fused.cache
def get_arr(bbox, time_of_interest, output_shape, nth_item=None, max_items=30):
    import fused
    import numpy as np
    utils = fused.load('https://github.com/fusedio/udfs/tree/9bfb5d0/public/Satellite_Greenest_Pixel').utils
    stac_items = utils.search_pc_catalog(bbox=bbox, time_of_interest=time_of_interest, query={"eo:cloud_cover": {"lt": 5}}, collection="sentinel-2-l2a")
    if not stac_items:return
    df_tiff_catalog = utils.create_tiffs_catalog(stac_items, ["B02", "B03", "B04", "B08"])
    if len(df_tiff_catalog)>max_items:
        raise ValueError(f'{len(df_tiff_catalog)} > max number of images ({max_items})')  
    if nth_item:
        if nth_item>len(df_tiff_catalog):
            raise ValueError(f'{nth_item} > total number of images ({len(df_tiff_catalog)})') 
        df_tiff_catalog = df_tiff_catalog[nth_item:nth_item+1]
        arrs_out = utils.run_pool_tiffs(bbox, df_tiff_catalog, output_shape)
        arr=arrs_out.squeeze()
    else:
        arrs_out = utils.run_pool_tiffs(bbox, df_tiff_catalog, output_shape)
        arr = utils.get_greenest_pixel(arrs_out, how='median', fillna=True)
    return arr
@fused.cache   
def df_to_hex(df, data_cols=['data'], h3_size=9, hex_col='hex', latlng_col=['lat','lng'], ordered=False, return_avg_lalng=True):
    duckdb_connect = fused.load(
            "https://github.com/fusedio/udfs/tree/a1c01c6/public/common/"
        ).utils.duckdb_connect
    agg_term = ', '.join([f'ARRAY_AGG({col}) as agg_{col}' for col in data_cols])
    if return_avg_lalng:
        agg_term+=f', avg({latlng_col[0]}) as {latlng_col[0]}_avg, avg({latlng_col[1]}) as {latlng_col[1]}_avg'
    qr = f"""
        SELECT h3_latlng_to_cell({latlng_col[0]}, {latlng_col[1]}, {h3_size}) AS {hex_col}, {agg_term}
        FROM df
        group by 1
    """
    if ordered:
        qr += '\norder by 1'
    con = duckdb_connect()
    return con.query(qr).df()
@fused.cache
def tile_to_df(bounds, arr, return_geometry=False):
    import numpy as np
    import pandas as pd
    if len(arr.shape)==2:
        arr = np.stack([arr])
    shape_transform_to_xycoor = fused.load(
        "https://github.com/fusedio/udfs/tree/a1c01c6/public/common/"
    ).utils.shape_transform_to_xycoor
    
    # calculate transform
    minx, miny, maxx, maxy = bounds.to_crs(3857).total_bounds
    dx = (maxx - minx) / arr.shape[-1]
    dy = (maxx - minx) / arr.shape[-2]
    transform = [dx, 0.0, minx, 0.0, -dy, maxy, 0.0, 0.0, 1.0]
    
    # calculate meshgrid
    x_list, y_list = shape_transform_to_xycoor(arr.shape[-2:], transform)
    X, Y = np.meshgrid(x_list, y_list)
    df = pd.DataFrame(X.flatten(), columns=["lng"])
    df["lat"] = Y.flatten()

    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils

    # convert back to 4326
    df = utils.geo_convert(df).set_crs(3857, allow_override=True).to_crs(bounds.crs)
    df["lat"]=df.geometry.y
    df["lng"]=df.geometry.x
    if not return_geometry:
        del df['geometry']
        
    # convert all the bands to dataframe
    for i,v in enumerate(arr):
        df[f"band{i+1}"] = v.flatten()
    return df


