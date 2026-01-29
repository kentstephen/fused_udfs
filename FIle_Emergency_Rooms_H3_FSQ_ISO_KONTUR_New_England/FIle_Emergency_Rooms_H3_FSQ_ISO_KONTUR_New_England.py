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
    # @fused.cache
    def get_df(bbox):
        return fused.run("fsh_2qOTeNbULsk7pQACLKQBOe", bbox=bbox)
    
    df = get_df(bbox)
    df = add_rgb(df, 'cnt')
    print(df)
    return df
