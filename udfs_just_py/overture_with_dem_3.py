# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds,
        res: int =None):
    import geopandas as gpd
    import pandas as pd
    from utils import get_over, run_query
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    # 1. Initial parameters
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    res_offset = 0  # lower makes the hex finer
    res = max(min(int(3 + zoom / 1.5), 12) - res_offset, 2)
    res=11
    print(res)
    utils = fused.load("https://github.com/fusedio/udfs/tree/e74035a1/public/common/").utils
    bounds = utils.bounds_to_gdf(bounds)
    # zoom = utils.estimate_zoom(bounds)
    df_buildings = get_over(bounds, overture_type="water")
    # print(df_buildings.columns)
   # df_places = get_over(bounds, overture_type="place")
    if df_buildings is None or df_buildings.empty:
        return
    
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    print(x, y, z)
    df_dem = fused.run("fsh_2bT8AoAunIsV5g7Rj4SC1B", x=x, y=y, z=z)
    print(df_buildings['geometry'])
    print(df_dem)
    # print(df)
    bounds = bounds.bounds.values[0]
    df = run_query(df_buildings, df_dem, res, bounds)
    print(df)
    return df