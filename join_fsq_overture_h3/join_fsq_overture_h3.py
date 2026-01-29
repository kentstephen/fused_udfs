@fused.udf
def udf(bounds: fused.types.Tile = None, resolution: int = 13):
    import geopandas as gpd
    import shapely
    from utils import get_fsq, join_h3_buildings_with_fsq
    # df_fsq = fused.run("UDF_Foursquare_Open_Source_Places", bounds=bounds, min_zoom=0)
    df_fsq = get_fsq(bounds)
    df_buildings = fused.run("fsh_38nTHEdsC9iTdLNWHiX8cE", bounds=bounds, resolution=resolution)
    gdf = join_h3_buildings_with_fsq(df_buildings, df_fsq, resolution)
    return gdf