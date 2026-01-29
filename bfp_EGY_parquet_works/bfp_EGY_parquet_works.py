@fused.udf
def udf(
        path: str = 's3://us-west-2.opendata.source.coop/vida/merged-google-microsoft-open-buildings/geoparquet/bfp_EGY.parquet',
        resolution: int = 8,
       min_count: int= 30):

    import duckdb
    import geopandas as gpd
    import shapely
    from shapely import wkt
    
    h3_utils = fused.load(
        "https://github.com/fusedio/udfs/tree/870e162/public/DuckDB_H3_Example/"
    ).utils
   
    # Create DuckDB connection
    con = duckdb.connect()
    
    # Load H3 functions into DuckDB
    h3_utils.load_h3_duckdb(con)
    
    # Fix the query syntax
    query = f"""
    SELECT
        h3_h3_to_string(h3_latlng_to_cell(ST_Y(ST_Centroid(geometry)), ST_X(ST_Centroid(geometry)), {resolution})) AS cell_id,
       -- h3_cell_to_boundary_wkt(cell_id) AS boundary,
        COUNT(1) AS cnt
    FROM read_parquet('{path}')
   
    GROUP BY cell_id
    HAVING cnt >= {min_count}
    """
    def get_data():
        df = con.sql(query).df()
        return df
    df = get_data()
    # # Convert to a GeoDataFrame and drop 'boundary'
    # gdf = gpd.GeoDataFrame(df, geometry=df['boundary'].apply(shapely.wkt.loads))
    # gdf = gdf.drop(columns=['boundary'])

    print(df)
    print(df['cnt'].describe())
    return df
