@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-08-20-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = 0, polygon: str = None, point_convert: str = None, land_class: str= None):
    from utils import get_overture, get_con, add_rgb, CMAP
    import pandas as pd
    school_gdf = get_overture(bbox=bbox, release=release, theme=theme, overture_type=overture_type, use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    # return gdf
    school_gdf['geometry'] = school_gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    school_df = pd.DataFrame(school_gdf)
    building_gdf = get_overture(bbox=bbox, release=release, theme="buildings", overture_type="building", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    building_gdf['geometry'] = building_gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    building_df = pd.DataFrame(building_gdf)
    # print(buildings_df["subtype"])
    con = get_con()
    # resolution = max(min(int(6 + (bbox.z[0] - 10) * (5/9)), 11), 0)
    resolution = 10
    query = """    
WITH school AS (
    SELECT       
        h3_latlng_to_cell(
            ST_Y(ST_Centroid(ST_GeomFromText(geometry))), 
            ST_X(ST_Centroid(ST_GeomFromText(geometry))), 
            $resolution
        ) AS cell,
        COUNT(1) AS school_count
    FROM school_df
    WHERE class = 'school'
    GROUP BY 1
), 
residential AS (
    SELECT       
        h3_latlng_to_cell(
            ST_Y(ST_Centroid(ST_GeomFromText(geometry))), 
            ST_X(ST_Centroid(ST_GeomFromText(geometry))), 
            $resolution
        ) AS cell,
        COUNT(1) AS residential_count
    FROM building_df
    WHERE subtype = 'residential'
    GROUP BY 1
)
SELECT
    h3_h3_to_string(COALESCE(r.cell, s.cell)) AS cell_id,
    COALESCE(r.residential_count, 0) AS residential_count,
    COALESCE(s.school_count, 0) AS school_count,
    CASE 
        WHEN COALESCE(s.school_count, 0) > 0 THEN 
            COALESCE(r.residential_count, 0)::float / s.school_count
        ELSE 0
    END AS ratio
FROM residential r
FULL OUTER JOIN school s ON r.cell = s.cell
WHERE COALESCE(r.residential_count, 0) > 0 OR COALESCE(s.school_count, 0) > 0   
""" 
    df = con.sql(query, params={'resolution': resolution}).df()
    df = add_rgb(df, 'ratio')  
    print(df.head(20))
    return df