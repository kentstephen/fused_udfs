# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection="3dep-seamless",
    band="data",
    res_factor:int=8,
    res:int=12,
    stats_type:str='mean',
    offset:int=10
):

    import odc.stac
    
    import palettable
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    # from utils import aggregate_df_hex, df_to_hex

    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    zoom = common.estimate_zoom(bounds)

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    search_results = catalog.search(
        collections=[collection],
        bbox=bounds,
        query={"gsd": {"eq": 10}}  # Add a query parameter to filter by GSD
    )
    items = search_results.item_collection()
    if not items or len(items) == 0:
        print("No items found for the given bounds and collection")
        return
    # print(items[0].assets.keys()) 
    # print(f"Returned {len(items)} Items")
    resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    print(f"{resolution=}")
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[band],
        resolution=resolution,
        bbox=bounds,
    ).astype(float)
    
    # Use data from the most recent time.
    arr = ds[band].max(dim="time")
    if arr is None:
        return

    
    df_latlng= common.arr_to_latlng(arr.values, bounds)
    
    df = aggregate_df_hex(bounds, df_latlng, res, latlng_cols=("lat", "lng"), offset=offset, stats_type=stats_type)
    print(df)
    return df


# @fused.cache()
def df_to_hex(bounds, df, res, latlng_cols=("lat", "lng"), offset:int=None):  
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    bounds_gdf = common.bounds_to_gdf(bounds) 
    bounds_values = bounds_gdf.bounds.values[0]
    xmin, ymin, xmax, ymax = bounds_values
    if offset==None:
       sql_string = "ARRAY_AGG(data) as agg_data"
    else:
        sql_string = f"array_agg(data + {offset}) as agg_data"
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
                    {sql_string}
            FROM df
            WHERE h3_cell_to_lat(hex) >= {ymin}
              AND h3_cell_to_lat(hex) < {ymax}
              AND h3_cell_to_lng(hex) >= {xmin}
              AND h3_cell_to_lng(hex) < {xmax}
            GROUP BY 1
        """
   
    con = common.duckdb_connect()
    return con.query(qr).fetchnumpy()
# @fused.cache
def aggregate_df_hex(bounds, df, res, latlng_cols=("lat", "lng"), offset=None, stats_type="mean"):
    import numpy as np
    import pandas as pd
    
    result = df_to_hex(bounds, df, res=res, latlng_cols=latlng_cols, offset=offset)
    
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
                         # 'agg_data': agg_data_arr, # keep if you need
                         'metric': metric
                                        })