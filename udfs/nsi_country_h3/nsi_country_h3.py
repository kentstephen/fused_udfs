@fused.udf
def udf(path: str='s3://fused-asset/infra/building_nsi_us/*.parquet', resolution: int= 7):
    import duckdb
    con = duckdb.connect()
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
                LOAD h3ext;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
           -- SET s3_region='us-west-2';""")
    
    query = f"""
    with to_cells as (
    select 
            h3_h3_to_string(h3_latlng_to_cell(y, x, {resolution})) AS cell_id,
            SUM(val_struct) as stats
    from read_parquet('{path}')
    group by 1
    )
    select 
    cell_id,
    stats / 10000 as stats
    from to_cells
    """
    df = con.sql(query)
    print(df)
    return df