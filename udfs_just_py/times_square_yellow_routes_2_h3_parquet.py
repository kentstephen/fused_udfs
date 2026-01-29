@fused.udf
def udf(path: str='s3://fused-users/stephenkentdata/times_square_yellow_routes_2_h3.parquet'):
    import geopandas as gpd
    import pandas as pd
    import duckdb
    from utils import add_rgb
    
    df = duckdb.sql(f"from read_parquet('{path}')").df()
    con = fused.utils.common.duckdb_connect()
    df = con.sql("select h3_h3_to_string(hex) as hex, cnt from df").df()
    print(df)
        # df = df.to_crs('EPSG:4326')
    # df = add_rgb(df, 'cnt')
    return df