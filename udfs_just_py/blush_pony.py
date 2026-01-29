@fused.udf
def udf(bounds: fused.types.Tile= None,
       resolution: int =10,
         costing = "auto", 
        time_steps = [30] ):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import get_fsq_isochrones_gdf, fsq_isochrones_to_h3, get_fsq

    # df_h3 = fused.run("fsh_1wv2pk3Sk3mkSagBnuqewj", 
    #                   bounds=bounds, 
    #                   resolution=resolution, 
    #                   res_offset=res_offset)
    gdf_fsq_isochrones = get_fsq_isochrones_gdf(costing, time_steps, bounds)
    gdf_fsq_isochrones['geometry'] = gdf_fsq_isochrones['geometry'].apply(shapely.wkt.dumps)
    
    # Better to use Pandas with DuckDB
    df_fsq_isochrones = pd.DataFrame(gdf_fsq_isochrones)
    if df_fsq_isochrones is None or df_fsq_isochrones.empty:
        return  
    bounds = bounds.bounds.values[0]
    df = fsq_isochrones_to_h3(df_fsq_isochrones, resolution, bounds)
    return df

