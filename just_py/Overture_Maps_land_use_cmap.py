@fused.udf 
def udf(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-10-23-0",
    theme: str = None,
    overture_type: str = "land_use", 
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None
):
    from utils import get_overture, add_rgb_cmap, CMAP
    
    gdf = get_overture(
        bbox=bbox,
        release=release,
        theme=theme,
        overture_type=overture_type,
        use_columns=use_columns,
        num_parts=num_parts,
        min_zoom=min_zoom,
        polygon=polygon,
        point_convert=point_convert
    )
    print(gdf["subtype"])
    gdf = add_rgb_cmap(gdf=gdf, key_field='subtype', cmap_dict=CMAP)
    return gdf