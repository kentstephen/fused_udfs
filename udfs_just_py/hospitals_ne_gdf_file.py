@fused.udf
def udf(bbox=None):
    import geopandas as gpd
    import shapely
    from utils import get_mask
    @fused.cache
    def get_hospitals():
        bbox = gpd.GeoDataFrame(
        geometry=[shapely.box(-73.63,42.64,-66.89,47.58)], # new england
        crs=4326
        )
        df = fused.run("fsh_3vO0E6CKOYOJqQv7LqUoPX", bbox=bbox)
        mask = get_mask()
        return gpd.overlay(df, mask, how='intersection')
    df = get_hospitals()
    print(type(df))
    return df
