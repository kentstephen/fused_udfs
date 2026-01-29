@fused.udf
def udf(bounds: fused.types.Tile = None, 
        place_type: str = "None",
        resolution: int = 13):
    import geopandas as gpd
    import shapely
    from utils import get_fsq, join_h3_buildings_with_fsq, add_rgb_cmap, CMAP
    # df_fsq = fused.run("UDF_Foursquare_Open_Source_Places", bounds=bounds, min_zoom=0)
    df_fsq = get_fsq(bounds, place_type)
    df_buildings = fused.run("fsh_38nTHEdsC9iTdLNWHiX8cE", bounds=bounds, resolution=resolution)
    if len(df_buildings) < 1:
        return
    df = join_h3_buildings_with_fsq(df_buildings, df_fsq, resolution)
    # print(df["level1_category_name"].unique())
    # df = add_rgb_cmap(df, 'level1_category_name', CMAP)
    return df