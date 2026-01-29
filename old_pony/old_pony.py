@fused.udf
def udf(bounds: fused.types.Bounds = None, resolution: int = 15):
    import pandas as pd
    common = fused.load("https://github.com/fusedio/udfs/tree/6dd2c4e/public/common/")
    tile = common.get_tiles(bounds, clip=True)
    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/")

    gdf = overture_maps.get_overture(bounds=tile, min_zoom=0)
    # gdf = gdf[gdf['level'].notna() & (gdf['level'] >= 1)].reset_index(drop=True)
    gdf = gdf[gdf.get('is_underground', False) != True]
    return gdf