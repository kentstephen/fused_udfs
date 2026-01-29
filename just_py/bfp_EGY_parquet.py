@fused.udf
def udf(path: str='s3://us-west-2.opendata.source.coop/vida/google-microsoft-open-buildings/geoparquet/by_country/country_iso=EGY/EGY.parquet',
        resolution: int=9):
    fused.options.request_timeout = 500
    import duckdb
    import geopandas as gpd
    from shapely import wkt
    import shapely
    con = duckdb.connect()
    con.sql("install 'httpfs'; load 'httpfs'; install spatial; load spatial; INSTALL h3 FROM community; LOAD h3;")

    df = con.sql(f"""WITH geo AS (
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
                    h3_h3_to_string(cell_id) AS cell_id,
                    h3_cell_to_boundary_wkt(cell_id) boundary,                   
                    SUM(cnt) AS cnt
                FROM to_cells
                GROUP BY ALL
       

                
                """).df()
    gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    print(gdf)
    return gdf