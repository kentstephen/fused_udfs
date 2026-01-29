@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    con = fused.utils.common.duckdb_connect()
    df = con.sql("from read_parquet(").df()
    return df

  