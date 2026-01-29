# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds,
        res: int =10):
    import geopandas as gpd
    import pandas as pd
    from utils import get_over, run_query
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    res_offset = 0  # lower makes the hex finer
   
    print(res)
    utils = fused.load("https://github.com/fusedio/udfs/tree/e74035a1/public/common/").utils
    bounds = utils.bounds_to_gdf(bounds)
    # zoom = utils.estimate_zoom(bounds)
    df_buildings = get_over(bounds, overture_type="building")
  
    if df_buildings is None or df_buildings.empty:
        return
    
    # x, y, z = tile.iloc[0][["x", "y", "z"]]
    # print(x, y, z)
    
    @fused.cache
    def get_dem(res):
        return fused.run("fsh_7SXAfMlrkS9twhxZiO2cup", bounds=bounds, res=res)
    df_dem = get_dem(res)
    # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=bounds, h3_size=res) hexify
    print(df_dem)
    # print(df)
    bounds = bounds.bounds.values[0]
    df = run_query(df_buildings, df_dem, res, bounds)
    print(df)
    return df