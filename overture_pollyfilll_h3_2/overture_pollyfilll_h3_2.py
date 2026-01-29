@fused.udf
def udf(bounds: fused.types.Tile = None, resolution: int = 14):
    import geopandas as gpd
    import shapely
    from utils import get_over, run_query
  
    df_buildings = get_over(bounds, overture_type="building")
   # df_places = get_over(bounds, overture_type="place")
    if df_buildings is None or df_buildings.empty:
        return
    # if df_places is None or df_places.empty:
    #     return
    bounds = bounds.bounds.values[0]
    df_fsq_h3 = fused.run("fsh_648WYjI5RVLg67O3qf4hWu", bounds=bounds, resolution=resolution)
    # print(df)
    df = run_query(df_buildings, df_fsq_h3, resolution, bounds)
    print(df)
    return df

    