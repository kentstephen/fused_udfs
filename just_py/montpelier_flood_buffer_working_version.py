@fused.udf
def udf(url: str='s3://us-west-2.opendata.source.coop/fused/overture/2024-03-12-alpha-0/theme=buildings/type=building/part=*/*.parquet',
        resolution: int=10):
    import geopandas as gpd
    from shapely.geometry import box
    from utils import get_water
    import shapely
    # Bbox for Montpelier and Barre
    bbox = box(-72.818472, 44.072055, -72.299101, 44.41482)
    
    # Extract coordinates
    xmin, ymin, xmax, ymax = bbox.bounds
    import duckdb
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
                LOAD h3ext;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_region='us-west-2';""")

    import duckdb
    run_query = fused.load(
            "https://github.com/fusedio/udfs/tree/43656f6/public/common/"
        ).utils.run_query
    query_1 = """
    CREATE OR REPLACE TABLE buildings AS
    WITH latlang as (
        SELECT       
            ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))) AS latitude,
            ST_X(ST_Centroid(ST_GeomFromWKB(geometry))) AS longitude
         FROM read_parquet($url)
          WHERE
        bbox.maxx >= $xmin
        AND bbox.minx <= $xmax
        AND bbox.miny <= $ymax
        AND bbox.maxy >= $ymin
    ), to_cells as (
        SELECT
        h3_latlng_to_cell(latitude, longitude, $resolution) AS cell_id,
        COUNT(1) as cnt
        FROM latlang
        GROUP BY 1
    ) SELECT
        h3_h3_to_string(cell_id) AS cell_id,
        SUM(cnt) as cnt
        FROM to_cells
        GROUP BY 1
    """
    
    result_df = get_water()   
    print(result_df)
    con.sql(query_1, params={'url': url, 'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax, 'resolution': resolution})
    query_2 ="""
      WITH w_buffer AS (
        SELECT
            ST_GeomFromText(geometry) AS water_buffer
        FROM result_df
    )
    SELECT
    
        b.cell_id AS cell_id,
       -- h3_cell_to_boundary_wkt(h3_string_to_h3(b.cell_id)) AS boundary,
        b.cnt
    FROM buildings b
    JOIN w_buffer w
    ON ST_Intersects(ST_GeomFromText(h3_cell_to_boundary_wkt(b.cell_id)), w.water_buffer)
    GROUP BY ALL;
    """ 


    df = con.sql(query_2).df()
 
    # gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    print(df)
    return df
    
