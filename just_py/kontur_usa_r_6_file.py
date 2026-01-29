@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import get_kontur, get_cells, add_rgb
    df_kontur = get_kontur()
    # print(df_kontur.columns)
    # print(df_kontur.head())
    # bounds = bbox.bounds.values[0]
    df = get_cells(df_kontur)
    print(df)
    # df = add_rgb(df, 'pop')
    return df