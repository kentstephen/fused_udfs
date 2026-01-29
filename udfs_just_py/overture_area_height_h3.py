@fused.udf
def udf(bounds: fused.types.Bounds = None, overture_type:str='building',
       res:int=11):
    import pandas as pd
    import duckdb
    from utils import df_to_hex

    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)
    bounds_gdf = common_utils.bounds_to_gdf(bounds)
    bounds_values = bounds_gdf.bounds.values[0]
    # df =df_to_hex(df=df,stats_type='mean')
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils
    
    gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=tile, overture_type=overture_type, min_zoom=0)
    if gdf is None or len(gdf)==0 or gdf.empty:
        return None
    gdf["area_m2"] = gdf.to_crs(gdf.estimate_utm_crs()).area.round(2)
    gdf['lat'] = gdf.geometry.centroid.y
    gdf['lng'] = gdf.geometry.centroid.x
    
    df_build = pd.DataFrame(gdf)
    
    del df_build['geometry']
    print(df_build.area_m2)
    df =df_to_hex(df=df_build, res=res, bounds_values=bounds_values)
    # df_dem = fused.run("fsh_2KKOTd6HSiGtNOYHLqG4xN", bounds=tile, res=res) # USGS dem_10meter_tile_hex_2
    df_dem = fused.run("fsh_3M3RyItkeAZpGR6fMZ482r", bounds=bounds, res=res) # jaxa  
    print(df_dem.columns)
    df_dem['metric'] = df_dem["metric"] - 2000
    df = duckdb.sql(" select df.*, coalesce(df.height,1) + df_dem.metric as total_elevation, df_dem.metric as elevation from df inner join df_dem on df.hex = df_dem.hex").df()
    print(df)
    return df


        