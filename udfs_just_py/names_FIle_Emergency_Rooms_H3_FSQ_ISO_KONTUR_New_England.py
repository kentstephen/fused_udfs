@fused.udf
def udf(resolution: int=8):
    import geopandas as gpd
    import shapely
    from utils import add_rgb
    bbox = gpd.GeoDataFrame(
    geometry=[shapely.box(-75.35,42.42,-66.22,47.77)], #new england
    crs=4326
)

    def get_df(bbox):
        return fused.run("fsh_6tJ4RL26bmtca8VvGDCU8i", 
                         bbox=bbox, resolution=resolution)
    
    df = get_df(bbox)

    df = add_rgb(df, 'cnt')
    return df
