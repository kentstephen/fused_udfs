
    
@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds= None, 
        res:int=5, 
        time_of_interest="1986-05-01/2986-10-30",

        stats_type:str="mean",
        png:bool=False,
        color_scale:float=1
):
    import numpy as np
    import rioxarray as rio
    import xarray as xr
    # Read tiles from S3 and return as image or H3
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)
    da = fused.run('fsh_56NrQ9oKzrRhluOgFdPIar', bounds=bounds, time_of_interest=time_of_interest) 
    if da is None:
        return
    # return arr
    print(da)
# If arr is a Dataset with a variable named "ndvi"
    da_tiff = da.rio.write_crs("EPSG:3857").rio.reproject("EPSG:4326")
    df_latlng = da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]
    # Load vector dataframe for each tile
    # da_tiff = load_from_url(url)
    # # If selected, returnn imagery
    # if png:
    #     return common.arr_to_plasma(arr, min_max=(-1000,2000/color_scale**0.5), colormap='plasma')
    # else:
    #     # Use your own res as UDF param or this formula
    #     if res is None:
    #         res_offset = 0  # lower makes the hex finer
    #         res = max(min(int(3 + z / 1.5), 12) - res_offset, 2)
    #     print(res)

# import rioxarray as rio
    # da_tiff = xr.DataArray(arr, dims=['y', 'x']).rio.write_crs("EPSG:3857").rio.reproject("EPSG:4326")
    # df_latlng = da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]
    # df_latlng = common.arr_to_latlng(arr,bounds)
    print(df_latlng)
    # return df_latlng
    # Create H3 hexagons with DuckDB and numpy
    df = aggregate_df_hex(tile, df_latlng, res, latlng_cols=("y", "x"), stats_type=stats_type)
    
    # # Change hexagon apearance in the visualization pallete
    print(df)
    return df


@fused.cache
def df_to_hex(tile, df, res, latlng_cols=("lat", "lng")): 
    #load common functions from GitHub
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    xmin, ymin, xmax, ymax = tile.geometry.iloc[0].bounds
    # create hexagons and collect the values in each cell as agg_data
    qr = f"""
        SELECT 
            h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            ARRAY_AGG(data) as agg_data
        FROM df
        WHERE
            h3_cell_to_lat(hex) >= {ymin} -- make sure we don't have overlap bewtween tiles
            AND h3_cell_to_lat(hex) < {ymax}
            AND h3_cell_to_lng(hex) >= {xmin}
            AND h3_cell_to_lng(hex) < {xmax}
        GROUP BY 1
        """
    con = common.duckdb_connect()
    return con.sql(qr).fetchnumpy() # return as a numpy array

@fused.cache
def aggregate_df_hex(tile, df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import numpy as np
    import pandas as pd
    # the result will be a numpy array which Fused can serialize
    # aggregation uses numpy as well
    result = df_to_hex(tile, df, res=res, latlng_cols=latlng_cols)

    # result is {'hex': array(...), 'agg_data': array of lists}
    hex_arr = result['hex']
    agg_data_arr = result['agg_data']

    # Apply numpy function to each list in the array
    if stats_type == "sum":
        metric = np.array([np.sum(x) for x in agg_data_arr])
    elif stats_type == "max":
        metric = np.array([np.max(x) for x in agg_data_arr]) 
    elif stats_type == "mean":
        metric = np.array([np.mean(x) for x in agg_data_arr])
    else:
        metric = np.array([np.mean(x) for x in agg_data_arr])

    return pd.DataFrame({
                        'hex': hex_arr,
                         # 'agg_data': agg_data_arr, # keep if you need: list of every value per cell
                         'metric': metric
                                        })

# @fused.cache
# def df_to_hex(df, res, latlng_cols=("lat", "lng")):  
#     common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
#     qr = f"""
#             SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, ARRAY_AGG(data) as agg_data
#             FROM df
#             group by 1
#           --  order by 1
#         """
#     con = common.duckdb_connect()
#     return con.sql(qr).df()


# @fused.cache
# def aggregate_df_hex(df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
#     import pandas as pd

#     df = df_to_hex(df, res=res, latlng_cols=latlng_cols)
#     if stats_type == "sum":
#         fn = lambda x: pd.Series(x).sum()
#     elif stats_type == "mean":
#         fn = lambda x: pd.Series(x).mean()
#     else:
#         fn = lambda x: pd.Series(x).mean()
#     df["metric"] = df.agg_data.map(fn)
#     del df['agg_data']
#     return df
def shape_transform_to_xycoor(shape, transform):
    import numpy as np
    n_y = shape[-2]
    n_x = shape[-1]
    w, _, x, _, h, y, _, _, _ = transform
    x_list = np.arange(x + w / 2, x + n_x * w + w / 2, w)[:n_x]
    y_list = np.arange(y + h / 2, y + n_y * h + h / 2, h)[:n_y]
    return x_list, y_list

def arr_to_latlng(arr, bounds):
    import numpy as np
    import pandas as pd
    from rasterio.transform import from_bounds
    from pyproj import Transformer
    
    transform = from_bounds(*bounds, arr.shape[-1], arr.shape[-2])
    x_list, y_list = shape_transform_to_xycoor(arr.shape[-2:], transform)
    X, Y = np.meshgrid(x_list, y_list)
    
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    lng_grid, lat_grid = transformer.transform(X, Y)
    
    df = pd.DataFrame(lng_grid.flatten(), columns=["lng"])
    df["lat"] = lat_grid.flatten()
    df["data"] = arr.flatten()
    return df