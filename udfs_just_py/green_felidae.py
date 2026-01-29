# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, buffer_multiple: float = 0.3):
    # Using common fused functions as helper
    # print(bounds)
    import pandas as pd
    common = fused.load("https://github.com/fusedio/udfs/tree/0b1bd10/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tiles = common.get_tiles(bounds, target_num_tiles=16)
    # Buffering tiles internally
    tiles.geometry = tiles.buffer(buffer_multiple / (tiles.z.iloc[0]) ** 2)
    bounds_df = pd.DataFrame([
        {'minx': geom.bounds[0], 
         'miny': geom.bounds[1], 
         'maxx': geom.bounds[2], 
         'maxy': geom.bounds[3]} 
        for geom in tiles.geometry
     ])
        # print(tiles.geometry.total_bouunds)
    # total_bounds = tiles.geometry.total_bounds
    # print(tiles.geometry.to_list())
    # tiles = [list(geom.bounds) for geom in tiles.geometry]
    # Use print statements to debug
    print(bounds_df)
    # print(bbox)
    # print(f"{tiles.geometry.area.sample(3)=}")
    return tiles
