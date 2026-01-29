# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, res:int=2, path: str='s3://fused-users/stephenkentdata/2025/hollywood_pois/hollywood.parquet'):
    import duckdb
    import numpy as np
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/acf1618/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tiles = common.get_tiles(bounds, target_num_tiles=16)
    df = duckdb.sql(f"from '{path}'").df()
    df['distance_log'] = np.log1p(df['distance']) 
    # print(df["distance"])
    # print(df['distance'].describe())
    df_hex = fused.run('fsh_4G4DetHkCMoj4hhYWQkNiP', res=res)
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
        # Connect to DuckDB
    con = utils.duckdb_connect()
    query = f"""select df.*, df_hex.cnt
                from df inner join df_hex on
              h3_latlng_to_cell(df.origin_latitude, df.origin_longitude, {res}) = df_hex.hex      
    """
    df = con.sql(query).df()
    print(df['cnt'].describe())
    return df