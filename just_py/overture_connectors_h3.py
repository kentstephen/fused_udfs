@fused.udf
def udf(bounds: fused.types.Tile = None, resolution: int = 13):
    import geopandas as gpd
    import shapely
    from utils import get_over, run_query
  
    df_conenctors = get_over(bounds, overture_type="connector")
   # df_places = get_over(bounds, overture_type="place")
    if df_conenctors is None or df_conenctors.empty:
        return
    # if df_places is None or df_places.empty:
    #     return
    bounds = bounds.bounds.values[0]
    # print(df)
    df = run_query(df_conenctors, resolution, bounds)
    print(df)
    return df

    