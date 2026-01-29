@fused.udf#(cache_max_age="0s")
def udf(
    bounds: fused.types.Bounds = None, 
    chip_len=256, # 256 is great for visualization
    visualize: bool = None,
    res:int=12,
    tree_scale: int = 20,
    show_dem: bool = True
    , # Change this to False when using this UDF for analysis
):
    """Visualize a given CHM"""
    import numpy as np
    import palettable
    import duckdb
    from utils import aggregate_df_hex, df_to_hex
    common = fused.load('https://github.com/fusedio/udfs/tree/36f4e97/public/common/').utils
    zoom = common.estimate_zoom(bounds)
    image_id = fused.run("UDF_Meta_CHM_tiles_geojson", bounds=bounds, use_centroid_method=False)
    path_of_chm = f"s3://dataforgood-fb-data/forests/v1/alsgedi_global_v6_float/chm/{image_id['tile'].iloc[0]}.tif"
    print(f"Using {path_of_chm=}")
    
    tile = common.get_tiles(bounds, clip=True)
    arr = common.read_tiff(tile, path_of_chm, output_shape=(chip_len, chip_len))    
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
        # bounds = utils.bounds_to_gdf(bounds)
        # bounds_values = bounds.bounds.values[0]
    df = utils.arr_to_latlng(arr, bounds)
    bounds_gdf = utils.bounds_to_gdf(bounds)
    bounds_values = bounds_gdf.bounds.values[0]
    df = aggregate_df_hex(df=df,stats_type='mean', res=res, bounds_values=bounds_values)
    df["elev_scale"] = int((15 - zoom) * 1)
    # import numpy as np
    df['viz_metric'] = np.where(df['canopy_height'] == 0, 0, np.sqrt(df['canopy_height']) * tree_scale)
    # # df['viz_metric'] = df["viz_metric"] - 1150
    # # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=bounds, h3_size=res) # hexify
    # # df_dem = fused.run("fsh_6vYIpdJfVam2KQZd3qTMA", bounds=bounds, res=res) # usgs 10 meter
    # if show_dem is True:
    df_dem = fused.run("fsh_3M3RyItkeAZpGR6fMZ482r", bounds=bounds, res=res) # jaxa
    # df_dem = fused.run("fsh_2KKOTd6HSiGtNOYHLqG4xN", bounds=bounds, res=res) # USGS dem_10meter_tile_hex_2

    df = duckdb.sql("""select df.hex, df.canopy_height,-- df_dem.metric as base_elevation,
                df_dem.metric +df.viz_metric as total_elevation,
            
               -- df.viz_metric as total_elevation, df.elev_scale
                from df inner join df_dem on df.hex=df_dem.hex
              -- where df.canopy_height = 0  """).df()
    # else: 
    #     df = duckdb.sql("""select df.hex, df.canopy_height,
             
    #             df.viz_metric as total_elevation 
    #             from df 
    #            -- where df.canopy_height > 0 """).df()
    # # df = df[df['base_elevation']>0]
    
    # df = df[df['metric']>0]
    print(df)
    return df
    # print(f"{arr.max()=}")

    # if visualize:
    #     vis = common.visualize(
    #         data=arr,
    #         mask = None,
    #         min=0,
    #         max=50,
    #         colormap=palettable.scientific.sequential.Bamako_20_r,
    #     )
    #     print(f"{vis=}")
    
    #     # unpacking bands
    #     return vis[:3]
    # else:
    #     return arr