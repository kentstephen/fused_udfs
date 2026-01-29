@fused.cache
def fetch_rgb_udf(
    bounds: fused.types.Bounds = None,
    neon_site: str = "SRER"
):
    import ee
    import numpy as np
    import xarray
    import xee

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)

    # Authenticate GEE
    key_path = "/mnt/cache/gp_creds.json"
    credentials = ee.ServiceAccountCredentials(
        "wgewneondataexplorer-7cd53ea0f@eminent-tesla-172116.iam.gserviceaccount.com", key_path
    )
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials)

    # Create collection
    geom = ee.Geometry.Rectangle(*bounds)
    scale = 1 / 2 ** max(0, zoom)

    # Get NEON RGB image
    ic = ee.ImageCollection("projects/neon-prod-earthengine/assets/RGB/001")\
        .filterDate("2017-01-01", "2018-01-01")\
        .filter(ee.Filter.eq("NEON_SITE", neon_site))

    # Open GEE object using xarray
    ds = xarray.open_dataset(ic, engine="ee", geometry=geom, scale=scale).isel(time=0)

    # Extract RGB bands
    R = ds["R"].values.squeeze().T.astype(float)
    G = ds["G"].values.squeeze().T.astype(float)
    B = ds["B"].values.squeeze().T.astype(float)

    # Replace invalid values with NaN
    R[R < 1] = np.nan
    G[G < 1] = np.nan
    B[B < 1] = np.nan

    # Stack RGB bands and clip values
    arr = np.stack([R, G, B], axis=0)
    arr_scaled = np.clip(arr, 1, 255).astype(np.uint8)

    return arr_scaled

def get_gee_credentials():
    import ee
    key_path = '/mnt/cache/gp_creds.json'
    credentials = ee.ServiceAccountCredentials("wgewneondataexplorer-7cd53ea0f@eminent-tesla-172116.iam.gserviceaccount.com", key_path)
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials)

# @fused.cache
def df_to_hex(df, res,bounds_values, latlng_cols=("lat", "lng")):
    xmin, ymin, xmax, ymax = bounds_values
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            array_agg(data) as agg_data
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
# @fused.cache
def aggregate_df_hex(df, res,bounds_values, latlng_cols=("lat", "lng"), stats_type="mean"):
    import pandas as pd
    import numpy as np
    
    # Convert to hexagons
    df = df_to_hex(df, res=res, bounds_values=bounds_values,latlng_cols=latlng_cols)
    
    # Define aggregation functions that handle null values
    if stats_type == "sum":
        fn = lambda x: pd.Series(x).fillna(0).sum()
    # elif stats_type == "mean":
    #     fn = lambda x: np.maximum(0, np.array([val for val in x if val is not None], dtype=float)).mean()
    else:
        fn = lambda x: pd.Series(x).mean()
    
    # Apply the aggregation function to create the metric column
    # df['canopy_height'] = df.agg_data.map(fn)
    # Instead of mean, try median which is more robust to outliers
    df['canopy_height'] = df.agg_data.map(lambda x: np.median(x[x > 1]) if len(x[x > 1]) > 0 else 0)
    # Replace any remaining NaN values with 0
    # df["metric"] = df["metric"].fillna(0)
    
    return df
