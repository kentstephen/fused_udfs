@fused.udf
def udf(bounds: fused.types.Tile = None, resolutionn: int = 9):
    import geopandas as gpd
    import shapely
    import duckdb
    from utils import get_overture, get_fsq, match_pois
    df_fsq = get_fsq(bounds)
    df_overture = get_overture(bounds, overture_type='place')
    # print(df_overture.names)

    # df = duckdb.sql("from df where same_cell = True").df()
    return df
    