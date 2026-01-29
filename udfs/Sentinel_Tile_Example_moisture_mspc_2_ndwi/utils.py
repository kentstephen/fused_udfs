visualize = fused.load(
    "https://github.com/fusedio/udfs/tree/2b25cb3/public/common/"
).utils.visualize

def get_over(bounds):
    import pandas as pd
    import shapely
    # overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=bounds, overture_type="land", min_zoom=0)
    if gdf is None or gdf.empty:
        return
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    return pd.DataFrame(gdf)

def df_to_hex(bounds, df, res, latlng_cols=("lat", "lng")):
    xmin, ymin, xmax, ymax = bounds
    # df_overture = get_over(bounds)
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            array_agg(data+5) as agg_data
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
    df = df_to_hex(bounds, df, res=res, latlng_cols=latlng_cols)
    
    # Define aggregation functions that handle null values
    if stats_type == "sum":
        fn = lambda x: pd.Series(x).fillna(0).sum()
    elif stats_type == "mean":
        fn = lambda x: pd.Series(x).mean()
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


