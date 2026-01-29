@fused.udf
def udf(bounds: fused.types.Tile= None, path: str = "s3://fused-users/stephenkentdata/kontur/kontur_world.parquet"):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import get_kontur, get_cells, add_rgb
    # df_kontur = get_kontur()
    # print(df_kontur.columns)
    # print(df_kontur.head())
    bounds = bounds.bounds.values[0]
    df = get_cells(bounds, path)
    print(df)
    # df = add_rgb(df, 'pop')
    print(df['hex'])
    return df