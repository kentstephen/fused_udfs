@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    @fused.cache
    def get_isochrones():
        con = fused.utils.common.duckdb_connect()
        query="""
        select ST_AsText(geometry) as geometry, * exclude(geometry) from read_parquet('s3://fused-users/stephenkentdata/ER_acutal_isochrones_new_eng.parquet')
        """
        df = con.sql(query).df()
        return gpd.GeoDataFrame(df.drop(columns=['geometry']), geometry=df.geometry.apply(shapely.wkt.loads))
    return get_isochrones()
