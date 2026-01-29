@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int= 11,h3_scale: int=1):
    import geopandas as gpd
    import shapely
    from utils import prep_for_duck, cell_convert, normalize_opacity
    # # 1. Set H3 resolution
    # x, y, z = bbox.iloc[0][["x", "y", "z"]]

    # if not resolution:
    #     resolution = max(min(int(3 + bbox.z[0] / 1.5), 12) - h3_scale, 2)
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, point_convert='True', use_columns = ["height"], min_zoom=0)
    if gdf_overture is None or gdf_overture.empty:
        return
    # print(gdf_overture)
    gdf_fsq = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    if gdf_fsq is None or gdf_fsq.empty:
        return

    df_overture = prep_for_duck(gdf_overture)
    df_fsq = prep_for_duck(gdf_fsq)
    df = cell_convert(df_overture, df_fsq, resolution)
    # df['a'] = normalize_opacity(df['cnt'])
    return df