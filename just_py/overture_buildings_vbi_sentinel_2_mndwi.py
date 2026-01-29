# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf(cache_max_age="0s")
def udf(bounds: fused.types.Bounds,
        res: int =11,
        time_of_interest="2024-06-01/2024-08-20",
        scale:float=0.099,
       ):
    import geopandas as gpd
    import pandas as pd
    import numpy as np
    from utils import get_over, run_query
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    res_offset = 0  # lower makes the hex finer
   
    print(res)
    utils = fused.load("https://github.com/fusedio/udfs/tree/e74035a1/public/common/").utils
    bounds = utils.bounds_to_gdf(bounds)
    # zoom = utils.estimate_zoom(bounds)
    # df_buildings = get_over(tile, overture_type="building")
  
    # if df_buildings is None or df_buildings.empty:
    #     return
    df_roads = get_over(tile, overture_type="building")
    if df_roads is None or df_roads.empty:
        return
    # df_dem = fused.run("fsh_65CrKEyQM7ePE0X7PtzKBR", bounds=bounds, res=res) #USGS
    # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) #hexify
    df_sentinel = fused.run("fsh_3HLae15V5acPSPS6Dqew1A", bounds=bounds, time_of_interest=time_of_interest, provider="MSPC", res=res)
    if df_sentinel is None or df_sentinel.empty:
        return
    print(df_sentinel)
    # print(df)
    bounds = bounds.bounds.values[0]
    gdf = run_query(df_roads, df_sentinel, res, bounds)
    # df['metric']= df['metric'] - 3000
    # gdf['elev_metric'] = np.exp((gdf['metric'] - 4.5) * 5) * 1000
    print(gdf)
    return gdf