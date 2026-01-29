@fused.udf
def udf( resolution: int=10,
        buffer_distance = 500):
    # url: str='s3://us-west-2.opendata.source.coop/fused/overture/2024-03-12-alpha-0/theme=buildings/type=building/part=*/*.parquet'
    import geopandas as gpd
    from utils import get_water, get_overture
    import duckdb
    from shapely import wkt, wkb
    
    # Define the bbox as a tuple (xmin, ymin, xmax, ymax)
    bbox = (-72.818472, 44.072055, -72.299101, 44.41482)
    xmin, ymin, xmax, ymax = bbox
    
    
    # Connect to DuckDB and load extensions only once
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql("""
        INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
        LOAD h3ext;
        INSTALL spatial;
        LOAD spatial;
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_region='us-west-2';
    """)
    
    # Combine queries to reduce intermediate results
    combined_query = """
    WITH buildings AS (
        SELECT
            h3_latlng_to_cell(
                ST_Y(ST_Centroid(ST_GeomFromText(geometry))),
                ST_X(ST_Centroid(ST_GeomFromText(geometry))),
                $resolution
            ) AS cell_id,
            COUNT(1) as cnt
        FROM buildings_df
        GROUP BY 1
    ),
    w_buffer AS (
        SELECT ST_GeomFromText(geometry) AS water_buffer
        FROM water_df
    )
    SELECT
        h3_h3_to_string(b.cell_id) AS cell_id,
        SUM(b.cnt) as cnt
    FROM buildings b
    JOIN w_buffer w
    ON ST_Intersects(ST_GeomFromText(h3_cell_to_boundary_wkt(h3_h3_to_string(b.cell_id))), w.water_buffer)
    GROUP BY 1
    """
    buildings_df = get_overture(bbox=bbox)
    
    
    water_df = get_water(bbox=bbox, buffer_distance=buffer_distance)
        
    # @fused.cache
    def execute_query():
        return con.sql(combined_query, params={
             'resolution': resolution
        }).df()
    
    df = execute_query()
    print(df)
    return df