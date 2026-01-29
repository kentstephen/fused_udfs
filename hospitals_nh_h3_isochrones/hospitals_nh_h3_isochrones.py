@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    import duckdb
    from utils import add_rgb
    @fused.cache
    def read_data():
        return duckdb.sql("from read_parquet('s3://fused-users/stephenkentdata/isochrones_parks_nyc_r_11.parquet')").df()
    df = read_data()
    df = add_rgb(df, 'cnt')
    return df

  