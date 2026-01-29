@fused.udf
def udf(path: str='s3://us-west-2.opendata.source.coop/boston-university/bu-glance/bu_glance_training_dataV1.parquet', res=5):
    import duckdb
    import pandas as pd
    import geopandas as gpd
    from shapely import wkt
    
   
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
            LOAD h3ext;
            INSTALL spatial;
            LOAD spatial;
            INSTALL httpfs;
            LOAD httpfs;""")
    query = f"""
        WITH h3_cells AS (
    SELECT
        h3_h3_to_string(h3_latlng_to_cell(Lat, Lon, {res})) AS cell_id,
        CAST(SUM(Veg_Density) as int) AS veg_density
    FROM
        read_parquet('{path}')

    WHERE Veg_Density IS NOT NULL
    GROUP BY
        cell_id
    )
    SELECT
        cell_id,
        h3_cell_to_boundary_wkt(cell_id) AS boundary,
        veg_density
    FROM
        h3_cells;

    """
    
    df = con.sql(query).df()
    gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(wkt.loads))
    print(gdf)
    return gdf

    
        
        
        
        
        
    