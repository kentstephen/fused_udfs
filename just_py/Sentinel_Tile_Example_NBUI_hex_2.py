@fused.udf
def udf(
    bounds: fused.types.Bounds,
    provider="MSPC",
    time_of_interest="2019-05-01/2019-07-30",
    res:int = 12
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
    # from utils import aggregate_df_hex

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
        green_band = "B03"  # Add this line
        red_band = "B04"
        nir_band = "B08"
        swir_band = "B12"
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
    else:
        raise ValueError(
            f'{provider=} does not exist. provider options are "AWS" and "MSPC"'
        )
    
    items = catalog.search(
        collections=["sentinel-2-l2a"],
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
                bands=[nir_band, swir_band, red_band, green_band],
                resolution=resolution,
                bbox=bounds,
            ).astype(float)

        # 
        ndbi = (ds[swir_band] - ds[nir_band]) / (ds[swir_band] + ds[nir_band])
        # mndwi = (ds[green_band] - ds[nir_band]) / (ds[green_band] + ds[nir_band])
            # One-line version of the NBUI formula
        L = 0.5  # Soil brightness correction factor (from SAVI)
        T = 1.0  # Normalization constant
        
        # nbui = ((ds[swir_band] - ds[nir_band])/(10.0 * (T + ds[swir_band])**0.5)) - \
        #        (((ds[nir_band] - ds[red_band]) * (1.0 + L))/(ds[nir_band] - ds[red_band] + L)) - \
        #        (ds[green_band] - ds[swir_band])/(ds[green_band] + ds[swir_band])
        # print(nbui.shape)  # Fixed: changed bui to nbui
        # print(bui.shape)
        arr = ndbi.max(dim="time")
        if arr is None:
            return
        utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
        # bounds = utils.bounds_to_gdf(bounds)
        # bounds_values = bounds.bounds.values[0]
        df = utils.arr_to_latlng(arr.values, bounds)
        # gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=bounds, overture_type="land")
        
        df = aggregate_df_hex(bounds, df=df,stats_type='mean', res=res)
        
        print(df.metric.describe())
        return df
        # rgb_image = common_utils.visualize(
        #     data=arr,
        #     min=0,
        #     max=1,
        #     colormap=['black', 'green']
        # )
        # return rgb_image

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
def aggregate_df_hex(bounds, df, res, latlng_cols=("lat", "lng"), stats_type="sum"):
    import pandas as pd
    import numpy as np
    
    # Convert to hexagons
    df = df_to_hex(bounds, df, res=res, latlng_cols=latlng_cols)
    
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

