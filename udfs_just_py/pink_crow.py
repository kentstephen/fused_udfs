@fused.udf
def udf(resolution: int=7,
       bbox: fused.types.TileGDF = None):
    import fused
    import geopandas as gpd
    import duckdb
    from utils import gdf_to_pandas_with_wkt, load_overture_gdf
    import duckdb
    
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
                LOAD h3ext;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
               -- SET s3_region='us-west-2';""")
    
    gdf = load_overture_gdf(
        bbox, overture_type="building", use_columns=use_columns
    )

    buildings_df = gdf_to_pandas_with_wkt(gdf)
    buildings_df = buildings_df.copy()
    
 
    query = f"""WITH geo AS (
                SELECT ST_Centroid(ST_GeomFromText(geometry)) AS geometry 
                FROM buildings_df
            ), to_cells as(
          
                SELECT 
                    h3_latlng_to_cell(ST_Y(geometry), ST_X(geometry), {resolution}) AS cell_id,
                    COUNT(1) as cnt
                FROM geo
                GROUP BY cell_id
           )     SELECT
                   h3_h3_to_string(cell_id) as cell_id,
                   SUM(cnt) as cnt
                FROM to_cells
                group by cell_id
                   
          
        """


    df = con.sql(query).df()

    print(df)
    return df