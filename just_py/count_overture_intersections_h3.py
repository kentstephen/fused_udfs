@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    from utils import get_con, load_overture_gdf
    resolution = max(min(int(6 + (bbox.z[0] - 10) * (5/9)), 11), 0)
    use_columns = ["geometry"]
    overture_table = load_overture_gdf(
        bbox, overture_type="connector", use_columns=use_columns
    )
    con = get_con()
    query = """    
    
        WITH latlang AS (
            SELECT       
                ST_Y(ST_GeomFromText(geometry)) AS latitude,
                ST_X(ST_GeomFromText(geometry)) AS longitude,
            FROM overture_table
            
        ), to_cells as(
            SELECT
                h3_h3_to_string(h3_latlng_to_cell(latitude, longitude, $resolution)) AS cell_id,
                COUNT(1) AS cnt
            FROM latlang
            GROUP BY 1
            ) select
                cell_id,
                SUM(cnt) as cnt
                from to_cells
                group by cell_id
        
        
            """
    df = con.sql(query, params={'resolution': resolution}).df()
    print(df)
    return df
    