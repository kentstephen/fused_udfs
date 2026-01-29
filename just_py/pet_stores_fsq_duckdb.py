@fused.udf
def udf(bbox: fused.types.Tile = None, resolution: int = 6):
    import geopandas as gpd
    import shapely
    from utils import get_fsq_points, run_query
    bounds = bbox.bounds.values[0]
    df_pet = get_fsq_points(bbox)

    df = run_query(df=df_pet, resolution=resolution, bounds=bounds)

    return df
