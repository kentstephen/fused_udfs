# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Tile=None,
        res: int = 13):
    import geopandas as gpd
    import pandas as pd
    from utils import get_over, run_query
    # utils = fused.load("https://github.com/fusedio/udfs/tree/e74035a1/public/common/").utils
    # bounds = utils.bounds_to_gdf(bounds)
    # zoom = utils.estimate_zoom(bounds)
    df_buildings = get_over(bounds, overture_type="building")
    # print(df_buildings)
   # df_places = get_over(bounds, overture_type="place")
    if df_buildings is None or df_buildings.empty:
        return
    
    # df_dem = fused.run("fsh_4bYjNAhhOKALqYMJAP4Sxh", bounds=bounds, h3_size=res)
    # print(df_dem.columns)
    # print(df)
    df_lc = fused.run("fsh_7AJES4TkAoyT3E0pfuwBZl",bounds=bounds, res=res)
    # print(df_lc)
    bounds = bounds.bounds.values[0]
    df = run_query(df_buildings, df_lc, res, bounds)
    print(df.groupby(['color','land_type'])['n_pixel'].sum().sort_values(ascending=False))
    return df