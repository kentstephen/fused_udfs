@fused.udf 
def udf(
    bbox: fused.types.TileGDF = None,
    release: str = "2025-01-22-0",
    theme: str = None,
    overture_type: str = None, 
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = 0,
    polygon: str = None,
    point_convert: str = None
):
    from utils import get_overture
    import geopandas as gpd
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
    @fused.cache
    def get_mask():
        gdf = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2023/FACES/tl_2023_24031_faces.zip')
        gdf = gdf.dissolve()
        gdf = gdf.to_crs('EPSG:4326')
        return gdf
        
    mask = get_mask()
    @fused.cache
    def mask_gdf(gdf, mask):
        return gpd.overlay(gdf, mask, how="intersection")  
    gdf= mask_gdf(gdf, mask)
    return gdf
