@fused.udf
def udf(n: int=10):
    import geopandas as gpd
    import shapely
    from utils import add_rgb
    import duckdb
    bbox = gpd.GeoDataFrame(
    geometry=[shapely.box(-75.35,42.42,-66.22,47.77)], #new england
    crs=4326
)
   
    def get_df(bbox):
        return fused.run("fsh_2l38399aMYrdtL75wOJJ7I", bbox=bbox)
    
    df = get_df(bbox)
    
    # df = duckdb.sql("from df where cnt is null and pop is not null").df()
    df = add_rgb(df, 'hospital_density')
    print(df)
    return df
