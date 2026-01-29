@fused.udf
def udf(bounds: fused.types.Tile = None, resolution: int = 13):
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
    # print(df)
    df = run_query(df_buildings, resolution, bounds)
    print(df)
    return df

    