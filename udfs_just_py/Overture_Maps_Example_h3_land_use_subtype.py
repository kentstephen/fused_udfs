@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-08-20-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = 0, polygon: str = None, point_convert: str = None, subtype: str= None):
    from utils import get_overture, add_rgb_cmap, CMAP
    import pandas as pd
    gdf = get_overture(bbox=bbox, release=release, theme=theme, overture_type=overture_type, use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    # return gdf
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    buildings_df = pd.DataFrame(gdf)
    # print(buildings_df["subtype"])
    duckdb_connect = fused.load(
            "https://github.com/fusedio/udfs/tree/a1c01c6/public/common/"
        ).utils.duckdb_connect
    con = duckdb_connect()
    # resolution = max(min(int(6 + (bbox.z[0] - 10) * (5/9)), 11), 0)
    resolution = 9
    query = """    
    
        WITH latlang AS (
            SELECT       
                ST_Y(ST_Centroid(ST_GeomFromText(geometry))) AS latitude,
                ST_X(ST_Centroid(ST_GeomFromText(geometry))) AS longitude,
                subtype
            FROM buildings_df
            WHERE subtype = $subtype
            
        ), to_cells as(
            SELECT
                h3_h3_to_string(h3_latlng_to_cell(latitude, longitude, $resolution)) AS cell_id,
                subtype,
                COUNT(1) AS cnt
            FROM latlang
            GROUP BY 1, 2
            ) select
                cell_id,
                subtype,
                SUM(cnt) as cnt
                from to_cells
                group by cell_id, subtype
        
        
            """ 
    df = con.sql(query, params={'subtype': subtype, 'resolution': resolution}).df()
    df = add_rgb_cmap(gdf=df, key_field="subtype", cmap_dict=CMAP)
    print(df)
    return df