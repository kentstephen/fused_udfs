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
        "time_of_interest": "2020-05/2020-08",
        "query": {
               "modis:horizontal-tile": {"gte": 0},  
        },
        "scale": 0.01,
    }
)
    
@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds= None, 
        res:int=5, 
        collection_params=modis_params,
        stats_type:str="mean",
        png:bool=False,
        color_scale:float=1
):

    # Read tiles from S3 and return as image or H3
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds)
    arr = fused.run('fsh_JSvmxVIAJTvikleG7l7NT', bounds=bounds, collection_params=collection_params,return_viz=False) 
    if arr is None:
        return
    # return arr
    print(arr)

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
    df_latlng = common.arr_to_latlng(arr, bounds)
    print(df_latlng)
    # return df_latlng
    # Create H3 hexagons with DuckDB and numpy
    df = aggregate_df_hex(tile, df_latlng, res, latlng_cols=("lat", "lng"), stats_type=stats_type)
    
    # # Change hexagon apearance in the visualization pallete
    print(df)
    return df


# def load_from_url(url):
#     # read Tiffs from AWS and return as a vector datfame
#     import rioxarray as rio
#     da_tiff = rio.open_rasterio(url).squeeze(drop=True).rio.reproject("EPSG:4326")
#     return da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]
    
# @fused.cache
def df_to_hex(tile, df, res, latlng_cols=("lat", "lng")): 
    #load common functions from GitHun
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

# @fused.cache
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