@fused.udf
def udf(bounds: fused.types.Bounds, stats_type="mean", type='hex', h3_size: int= 13,color_scale:float=1):
    import pandas as pd
    import rioxarray
    from utils import aggregate_df_hex, url_to_plasma

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)
    zoom = common_utils.estimate_zoom(bounds)
    # 1. Initial parameters
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    url = f"https://s3.amazonaws.com/elevation-tiles-prod/geotiff/{z}/{x}/{y}.tif"
    if type=='png':
        return url_to_plasma(url, min_max=(-1000,2000/color_scale**0.5), colormap='plasma')
    else:
        
        res_offset = -1 # lower makes the hex finer
        # h3_size = max(min(int(3 + zoom / 1.5), 12) - res_offset, 2)
        print(h3_size)
    
        # 2. Read tiff
        da_tiff = rioxarray.open_rasterio(url).squeeze(drop=True).rio.reproject("EPSG:4326")
        df_tiff = da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]
    
        # 3. Hexagonify & aggregate
        df = aggregate_df_hex(
            df_tiff, h3_size, latlng_cols=["y", "x"], stats_type=stats_type
        )
        df["elev_scale"] = int((15 - z) * 1)
        # df_building = fused.run("fsh_3o1ev6ebOHwIC20CPC28Sz", bounds=bounds, res=h3_size)
        # if df_building is not None and not df_building.empty:
        #     # Get unique hex values from df_building
        #     building_hex_values = df_building['hex'].unique()
            
        #     # Create a dictionary mapping hex values to heights
        #     hex_to_height = dict(zip(df_building['hex'], df_building['height']))
            
        #     # Apply heights and update metric for matching hex values
        #     for hex_value in building_hex_values:
        #         # Create mask for matching hex values
        #         mask = df['hex'] == hex_value
                
        #         # Add height column for matching rows
        #         df.loc[mask, 'height'] = hex_to_height.get(hex_value, 0)
                
        #         # Add 5 to the metric value for all matching rows
        #         df.loc[mask, 'metric'] += 5
                
        #         # Calculate total_elevation by adding metric and height
        #         df.loc[mask, 'metric'] = df.loc[mask, 'metric'] + df.loc[mask, 'height']
        # df_road = fused.run("fsh_6N6n5fHUn7U1g2MLsM0w45", bounds=bounds, res=h3_size)
        # if df_road is not None and not df_road.empty:
        #     road_hex_values = df_road['hex'].unique()
        #         # road_hex_values = set(df_building['hex'].unique())
        
        # # Create a boolean mask for all matching hex values at once
        #     mask = df['hex'].isin(building_hex_values)
        #     building_hex_values = set(df_building['hex'].unique())
        
        # # Create a boolean mask for all matching hex values at once
        #     mask = df['hex'].isin(road_hex_values)
        
        # # Add 5 to the metric value for all matching rows in one operation
        #     df.loc[mask, 'metric'] = df.loc[mask, 'metric'] + 5
        
    # Add 5 to the metric value for all matching rows in one operation
    # df.loc[mask, 'metric'] = df.loc[mask, 'metric'] + 5
        df["metric"]=df["metric"]*color_scale
        df['metric'] = df["metric"] - 3000 
        return df
