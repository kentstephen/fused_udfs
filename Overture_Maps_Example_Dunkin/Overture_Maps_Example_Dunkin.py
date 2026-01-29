@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-09-18-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = 0, polygon: str = None, point_convert: str = None):
    from utils import get_overture
    gdf = get_overture(bbox=bbox, release=release, theme="places", overture_type="place", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    
    mask = gdf['names'].apply(lambda x: isinstance(x, dict) and "Starbucks" in x.get('primary', ''))
    gdf = gdf[mask]
    return gdf