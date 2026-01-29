# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection="3dep-seamless",
    band="data",
    res_factor:int=15,
    res:int=11
):

    import odc.stac
    
    import palettable
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    # from utils import aggregate_df_hex, df_to_hex

    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)

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

    # Filter for 10m GSD but preserve STAC item objects
    # items = [item for item in all_items if item.properties["gsd"] == 10]
    
    # Create a new ItemCollection with just the filtered items
    # items = ItemCollection(filtered_items)
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
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    # bounds = utils.bounds_to_gdf(bounds)
    # bounds_values = bounds.bounds.values[0]
    df = utils.arr_to_latlng(arr.values, bounds)
    bounds = utils.bounds_to_gdf(bounds) 
    bounds = bounds.bounds.values[0]

    df = aggregate_df_hex(bounds,df=df,stats_type='mean', res=res)
    # df['metric'] = df["metric"] - 800 
    # print(df)
    # df = df[df['metric']>0]
    # df = df[df['metric']>813]
    print(df['metric'].describe())
    # df["elev_scale"] = int((15 - zoom) * 1)
    return df
    

    # rgb_image = utils.visualize(
    #     data=arr,
    #     min=60,
    #     max=900,
    #     colormap=palettable.matplotlib.Viridis_20,
    # )
    # return rgb_image

def df_to_hex(bounds, df, res, latlng_cols=("lat", "lng")):
    xmin, ymin, xmax, ymax = bounds
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            array_agg(data+10) as agg_data
            FROM df
            where
    h3_cell_to_lat(hex) >= {ymin}
    AND h3_cell_to_lat(hex) < {ymax}
    AND h3_cell_to_lng(hex) >= {xmin}
    AND h3_cell_to_lng(hex) < {xmax}

            group by 1
          -- order by 1
        """
    con = utils.duckdb_connect()
    return con.query(qr).df()
    

    # return con.query(qr).df()
def aggregate_df_hex(bounds, df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import pandas as pd
    import numpy as np
    
    # Convert to hexagons
    df = df_to_hex(bounds,df, res=res, latlng_cols=latlng_cols)
    
    # Define aggregation functions that handle null values
    if stats_type == "sum":
        fn = lambda x: pd.Series(x).fillna(0).sum()
    # elif stats_type == "mean":
    #     fn = lambda x: np.maximum(0, np.array([val for val in x if val is not None], dtype=float)).mean()
    else:
        fn = lambda x: pd.Series(x).mean()
    
    # Apply the aggregation function to create the metric column
    df["metric"] = df.agg_data.map(fn)
    del df['agg_data']
    # Replace any remaining NaN values with 0
    # df["metric"] = df["metric"].fillna(0)
    
    return df