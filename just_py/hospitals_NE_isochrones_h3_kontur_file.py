@fused.udf
def udf(n: int=10):
    import geopandas as gpd
    import shapely
    from utils import add_rgb
    bbox = gpd.GeoDataFrame(
    geometry=[shapely.box(-75.35,42.42,-66.22,47.77)], #new england
    crs=4326
)
   
    def get_df(bbox):
        return fused.run("fsh_2v638F3b5Otl8ctFejQ9FG", bbox=bbox)
    df = get_df(bbox)
    # df = add_rgb(df, 'cnt')
    return df
