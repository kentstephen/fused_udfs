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
        df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
        return df[df['fsq_category_ids']=='4bf58dd8d48988d194941735']
        # mask = get_mask()
        # return gpd.overlay(df, mask, how='intersection')
        
    df = get_hospitals()
    print(df['name'])
    return df
