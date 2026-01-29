@fused.udf
def udf(bounds: fused.types.Tile= None, 
        res_offset : int = 0, 
        resolution: int = 8):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import get_fsq, run_query
    
    df_kontur = fused.run("fsh_1Kg290kb0BPNahbrby4Kah", bounds=bounds)
    if df_kontur is None or df_kontur.empty:
        return pd.DataFrame({})
    df_fsq = get_fsq(bounds)
    if df_fsq is None or df_fsq.empty:
        return pd.DataFrame({})
    bounds = bounds.bounds.values[0]
    df= run_query(df_kontur, df_fsq, resolution, res_offset, bounds)
    print(df)
    return df

    