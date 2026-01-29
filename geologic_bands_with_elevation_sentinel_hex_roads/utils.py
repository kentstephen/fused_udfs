def get_greenest_pixel(arr_rgbi, how="median", fillna=True):
    import numpy as np
    import pandas as pd

    # First 3 bands to visualize, last 2 bands to calculate NDVI
    out = (arr_rgbi[-1] * 1.0 - arr_rgbi[-2] * 1.0) / (
        arr_rgbi[-1] * 1.0 + arr_rgbi[-2] * 1.0
    )
    t_len = out.shape[0]
    out_flat = out.reshape(out.shape[0], out.shape[1] * out.shape[2])
    # Find greenest pixels
    sorted_indices = np.argsort(out_flat, axis=0)
    if how == "median":
        median_index = sorted_indices[t_len // 2]
    elif how == "min":
        median_index = np.argmin(out_flat, axis=0)
    else:
        median_index = np.argmax(out_flat, axis=0)

    out_flat = out_flat[median_index, np.arange(out.shape[1] * out.shape[2])]

    output_bands = []

    for b in [0, 1, 2]:
        out_flat = arr_rgbi[b].reshape(t_len, out.shape[1] * out.shape[2])

        # Replace 0s with NaNs
        out_flat = np.where(out_flat == 0, np.nan, out_flat)
        if fillna:
            out_flat = pd.DataFrame(out_flat).ffill().bfill().values
        out_flat = out_flat[median_index, np.arange(out.shape[1] * out.shape[2])]
        output_bands.append(out_flat.reshape(out.shape[1], out.shape[2]))
    return np.stack(output_bands)
def get_arr(
    bounds,
    time_of_interest,
    output_shape,
    nth_item=None,
    max_items=100
):

    greenest_example_utils = fused.load('https://github.com/fusedio/udfs/tree/e74035a/public/Satellite_Greenest_Pixel').utils

    stac_items = greenest_example_utils.search_pc_catalog(
        bounds=bounds,
        time_of_interest=time_of_interest,
        query={"eo:cloud_cover": {"lt": 5}},
        collection="sentinel-2-l2a"
    )
    if not stac_items: return
    df_tiff_catalog = greenest_example_utils.create_tiffs_catalog(stac_items, ["B04", "B03", "B02"])
    if len(df_tiff_catalog) > max_items:
        raise ValueError(f'{len(df_tiff_catalog)} > max number of images ({max_items})')  
    if nth_item:
        if nth_item > len(df_tiff_catalog):
            raise ValueError(f'{nth_item} > total number of images ({len(df_tiff_catalog)})') 
        df_tiff_catalog = df_tiff_catalog[nth_item:nth_item + 1]
        arrs_out = greenest_example_utils.run_pool_tiffs(bounds, df_tiff_catalog, output_shape)
        arr = arrs_out.squeeze()
    else:
        arrs_out = greenest_example_utils.run_pool_tiffs(bounds, df_tiff_catalog, output_shape)
        arr = get_greenest_pixel(arrs_out, how='median', fillna=True)
    return arr
def df_to_hex(bounds_values,df, data_cols=['data'], res=None, hex_col='hex', latlng_col=['lat','lng'], ordered=False, return_avg_lalng=False):
    xmin, ymin, xmax, ymax = bounds_values
    duckdb_connect = fused.load(
            "https://github.com/fusedio/udfs/tree/a1c01c6/public/common/"
        ).utils.duckdb_connect
    agg_term = ', '.join([f'ARRAY_AGG({col}) as agg_{col}' for col in data_cols])
    if return_avg_lalng:
        agg_term+=f', avg({latlng_col[0]}) as {latlng_col[0]}_avg, avg({latlng_col[1]}) as {latlng_col[1]}_avg'
    qr = f"""
        SELECT h3_latlng_to_cell({latlng_col[0]}, {latlng_col[1]}, {res}) AS {hex_col}, {agg_term}
        FROM df
        where
    h3_cell_to_lat(hex) >= {ymin}
    AND h3_cell_to_lat(hex) < {ymax}
    AND h3_cell_to_lng(hex) >= {xmin}
    AND h3_cell_to_lng(hex) < {xmax}
        group by 1
    """
    if ordered:
        qr += '\norder by 1'
    con = duckdb_connect()
    return con.query(qr).df()

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
road_cmap = {
    'motorway': (255, 0, 0),         # Red
    'primary': (255, 128, 0),        # Orange
    'secondary': (255, 255, 0),      # Yellow
    'tertiary': (0, 0, 255),       # blue
    'residential': (0, 255, 0),      # Green
    'living_street': (0, 255, 170),  # Mint green
    'trunk': (0, 255, 255),          # Cyan
    'unclassified': (0, 170, 255),   # Light blue
    'service': (170, 255, 0),          # lime
    'pedestrian': (85, 0, 255),      # Blue-violet
    'footway': (170, 0, 255),        # Violet
    'steps': (255, 0, 255),          # Magenta
    'path': (255, 0, 170),           # Pink
    'track': (255, 0, 85),           # Rose
    'cycleway': (128, 128, 255),     # Light purple
    'bridleway': (128, 255, 128),    # Light green
    'unknown': (128, 128, 128)       # Gray
}


