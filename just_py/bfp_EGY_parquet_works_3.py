@fused.udf
def udf(path: str='s3://us-west-2.opendata.source.coop/vida/merged-google-microsoft-open-buildings/geoparquet/bfp_BRA.parquet',
        resolution: int=9):
    fused.options.request_timeout = 500
    import duckdb
    run_query = fused.load(
            "https://github.com/fusedio/udfs/tree/43656f6/public/common/"
        ).utils.run_query
    query = f"""WITH geo AS (
                SELECT ST_Centroid(ST_GeomFromWKB(geometry)) AS geometry 
                FROM read_parquet('{path}')
            ),
            to_cells AS (
                SELECT 
                    h3_latlng_to_cell(ST_Y(geometry), ST_X(geometry), {resolution}) AS cell_id,
                    COUNT(1) AS cnt
                FROM geo
                GROUP BY cell_id
            )
            SELECT 
                cell_id,
                SUM(cnt) AS cnt
            FROM to_cells
            GROUP BY cell_id
"""

    # @fused.cache
    def inner(query):
        return run_query(query, return_arrow=False)

    df = inner(query)

    print(df)
    return df