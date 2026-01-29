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
        return fused.run("fsh_4tdwX6qZ3gnv7RWaPTRGND", bbox=bbox)
    
    df = get_df(bbox)
    df = add_rgb(df, 'pop')
    
    df['nearby_ERs'] = df['nearby_ERs'].to_list()
    
    print(df)
    return df
