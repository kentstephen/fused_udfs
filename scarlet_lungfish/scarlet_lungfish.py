# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, path: str='s3://fused-users/stephenkentdata/2025/hollywood_pois/hollywood.parquet', res:int= 4):
    # Using common fused functions as helper
    import duckdb
    def read_data(path):
        return duckdb.sql(f"from read_parquet('{path}')").df()
    df_poi = read_data(path)

    def get_cells(df_poi):
        utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
        # Connect to DuckDB
        con = utils.duckdb_connect()
        query=f"""
   
        select
        h3_latlng_to_cell(origin_latitude, origin_longitude, {res}) as hex,
        count(1) as cnt
        from df_poi
        group by 1

        """
        return con.sql(query).df()
    df = get_cells(df_poi)
    print(df['cnt'].describe())
    return df
    