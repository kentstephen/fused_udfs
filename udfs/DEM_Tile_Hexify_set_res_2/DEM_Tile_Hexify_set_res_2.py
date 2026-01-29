@fused.udf
def udf(bounds: fused.types.Bounds, 
        stats_type="mean", 
        h3_size: int=14, 
        type='hex', 
        color_scale:float=1):
    import pandas as pd
    import rioxarray
    from utils import aggregate_df_hex, url_to_plasma
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils

    # convert bounds to tile
    zoom = utils.estimate_zoom(bounds)
    tile = utils.get_tiles(bounds, zoom=zoom)


    # conn = utils.duckdb_connect()

    # 1. Set H3 resolution
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    # 1. Initial parameters
    # x, y, z = bounds.iloc[0][["x", "y", "z"]]
    url = f"https://s3.amazonaws.com/elevation-tiles-prod/geotiff/{z}/{x}/{y}.tif"
    if type=='png':
        return url_to_plasma(url, min_max=(-1000,2000/color_scale**0.5), colormap='plasma')
    else:
        
        res_offset = 0  # lower makes the hex finer
        h3_size = max(min(int(3 + bounds.z[0] / 1.5), 12) - res_offset, 2)
        print(h3_size)
    
        # 2. Read tiff
        da_tiff = rioxarray.open_rasterio(url).squeeze(drop=True).rio.reproject("EPSG:4326")
        df_tiff = da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]
        bounds = bounds.bounds.values[0]
        # 3. Hexagonify & aggregate
        df = aggregate_df_hex(
            bounds, df_tiff, h3_size, latlng_cols=["y", "x"], stats_type=stats_type
        )
        df["elev_scale"] = int((15 - z) * 1)
        df["metric"]=df["metric"]*color_scale
        df = df[df["metric"] > 0]
        # df = df.rename(columns={'metric': 'elevation'})
        return df
