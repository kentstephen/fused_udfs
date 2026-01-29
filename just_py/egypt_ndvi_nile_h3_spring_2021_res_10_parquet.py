@fused.udf
def udf(path: str='s3://fused-users/stephenkentdata/stephenkentdata/2025/egypt_delta_ndvi/egypt_ndvi_nile_h3_spring_2021_res_10.parquet'):
    import geopandas as gpd
    import pandas as pd
    try:
        df = gpd.read_parquet(path)
        df = df.to_crs('EPSG:4326')
    except:
        df = pd.read_parquet(path)
    print(df)
    return df