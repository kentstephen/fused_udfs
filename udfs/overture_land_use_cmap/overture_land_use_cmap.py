# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, buffer_multiple: float = 1):
    from utils import add_rgb_cmap, CMAP
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/beb4259/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tile = common.get_tiles(bounds, target_num_tiles=16)
    # Buffering tiles internally
    gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=tile, overture_type='land_use', min_zoom=0)
    gdf = add_rgb_cmap(gdf=gdf, key_field='subtype', cmap_dict=CMAP)
    return gdf
    
    
