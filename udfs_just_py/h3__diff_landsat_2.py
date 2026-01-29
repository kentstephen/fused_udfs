@fused.udf
def udf(bounds: fused.types.Bounds= None,
        first_time_period:str = "1991-05-01/1993-10-30",
        second_time_period: str= "2022-05-01/2024-10-30",
       res:int=5
    ):
    import pandas as pd
    import duckdb


    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    landsat_h3_1 = "fsh_3GuMWWpbjtBTz0XoF05Fgm"
    landsat_h3_2 = "fsh_7Cdc2YEbBwzXUv1hhfbVIr"
    
    # df_first, df_second = common.run_pool(
    #     lambda args: fused.run(
    #         args[0],  # dataset token (landsat_h3, sentinel_h3, etc.)
    #         bounds=bounds, 
    #         res=res, 
    #         time_of_interest=args[1]  # time period
    #     ),
    #     [(landsat_h3_1, first_time_period), (landsat_h3_2, second_time_period)],
    #     max_workers=2
    # )
    if df_first is None or df_second is None or df_first.empty or df_second.empty:
        return
    # df = duckdb.sql("""
    # select 
    #     df_first.hex, 
    #     ((df_second.metric - df_first.metric) / df_first.metric) * 100 as metric
    # from df_first 
    # inner join df_second on df_first.hex = df_second.hex
    # where df_first.metric != 0
    # """).df()
    
    df = duckdb.sql("""
    select 
    df_first.hex, 
    (df_second.metric - df_first.metric)*100 as pct_change_ndvi,
        --(df_second.metric - df_first.metric) * 10_000 as elev_metric,
    
    from df_first 
    inner join df_second 
    on df_first.hex = df_second.hex""").df()
    # df['metric'] = df['metric']-1
    return df
    # gdf = run_query(df, res, bounds)
    # gdf['metric'] = gdf['metric'] - 1
    # gdf['metric'] = gdf['metric'] * 100
    # return gdf




def run_query(df, res, bounds):
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    import shapely
    import geopandas as gpd
    gdf_arrow = get_overture_arrow(bounds)
    
    con = common.duckdb_connect()
    con.sql("CALL register_geoarrow_extensions()")
    qr =f"""  
    
    with geo_to_cells as(
    select
    unnest(h3_polygon_wkt_to_cells_experimental(st_astext(geometry), {res}, 'overlap')) as hex,
    geometry,
   -- names.primary as name
    from gdf_arrow)
    select
    st_astext(g.geometry) as boundary,
 --   g.name,
    
    avg(d.metric) as metric
    from geo_to_cells g inner join df d on g.hex = d.hex
    group by 1
    
    """
    df = con.sql(qr).df()
    return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    


def get_overture_arrow(bounds):
    import pandas as pd
    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/")
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)
   # con.execute("")
    
    # Get your data
    gdf = overture_maps.get_overture(bounds=tile, overture_type="land_use", min_zoom=0)
    # gdf_b = overture_maps.get_overture(bounds=tile, min_zoom=0)
    # gdf = overture_maps.get_overture(bounds=tile, overture_type="land_use", min_zoom=0)
    # gdf_i = overture_maps.get_overture(bounds=tile, overture_type="infrastructure", min_zoom=0)
    # # return gdf
    # gdf = pd.concat([gdf_land, gdf_i])
    return gdf.to_arrow()
    # con = common.duckdb_connect()
    # Now DuckDB can query it directly
    # df_o = con.sql("SELECT * FROM gdf_arrow").df()
   
