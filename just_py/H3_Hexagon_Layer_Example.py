# Note: This UDF is only for demo purposes. You may get `HTTP GET error` after several times calling it. This is the data retrieval issue caused by Cloudfront servers not responding.
@fused.udf
def udf(bbox=None, resolution: int = 14, min_count: int = 1):
    import duckdb
    import shapely
    import geopandas as gpd
    common = fused.load("https://github.com/fusedio/udfs/tree/6e8abb9/public/common/")
    con = common.duckdb_connect()

    con.sql(f"""INSTALL httpfs; LOAD httpfs;""") 
    
    @fused.cache
    def read_data(url, resolution, min_count):
        df = con.sql("""
        SELECT h3_latlng_to_cell(dropoff_latitude, dropoff_longitude, $resolution) as cell_id,
                --    h3_cell_to_lat(cell_id) as pu_cell_lat,
                  --  h3_cell_to_lng(cell_id) as pu_cell_lng,
               count(1) as cnt
        FROM read_parquet($url) 
        GROUP BY cell_id
        HAVING cnt>$min_count
        """, params={'url': url, 'resolution': resolution, 'min_count': min_count}).df()
        return df

    df = read_data('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2010-01.parquet', resolution, min_count)
    print("number of trips:", df.cnt.sum())
    print(df)
    return df
