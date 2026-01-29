@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-08-20-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = None, polygon: str = None, point_convert: str = None):
    from utils import get_overture,CMAP, add_rgb_to_gdf
    gdf = get_overture(bbox=bbox, release=release, theme=theme, overture_type="infrastructure", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)

    gdf = add_rgb_to_gdf(gdf=gdf, cmap=CMAP, attr='subtype')
    # 

    print(gdf.columns)
    try:
        return gdf
    except exception as e:
        if e.code == 422:
            pass
