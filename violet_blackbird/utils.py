import duckdb
def get_bbox():
    con = duckdb.connect()
    con.sql("install spatial; load spatial; install httpfs; load httpfs;")
    bbox, wkt_geom = con.sql("""
    select
        bbox,
        ST_AsText(ST_GeomFromWKB(geometry)) AS wkt_geom
    from read_parquet('s3://overturemaps-us-west-2/release/2024-06-13-beta.0/theme=divisions/type=division_area/*', filename=true, hive_partitioning=1)
    where
        country = 'US' 
        and subtype = 'country' 
      --  and region = 'US-CA'
       
        
    """
    ).fetchall()[0]
    return bbox, wkt_geom