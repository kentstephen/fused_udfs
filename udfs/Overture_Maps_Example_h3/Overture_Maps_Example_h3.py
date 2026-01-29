@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-03-12-alpha-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = None, polygon: str = None, point_convert: str = None,
       resolution: int=9):
    from utils import get_overture, get_con
    from shapely import wkt
    import pandas as pd
    import duckdb
    import geopandas as gpd
    import shapely
    gdf = get_overture(bbox=bbox, release=release, theme=theme, overture_type=overture_type, use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    buildings_df = pd.DataFrame(gdf)
    con = get_con()
    buildings_query = """    
    
        WITH latlang AS (
            SELECT       
                ST_Y(ST_Centroid(ST_GeomFromText(geometry))) AS latitude,
                ST_X(ST_Centroid(ST_GeomFromText(geometry))) AS longitude,
            FROM buildings_df
            
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
    df = con.sql(buildings_query, params={'resolution': resolution}).df()
    print(df)
    return df

    