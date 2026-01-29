@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-08-20-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = 0, polygon: str = None, point_convert: str = None):
    from utils import get_overture, get_con, add_rgb_cmap, CMAP
    import pandas as pd
    gdf = get_overture(bbox=bbox, release=release, theme=theme, overture_type=overture_type, use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    # return gdf
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    buildings_df = pd.DataFrame(gdf)
    # print(buildings_df["subtype"])
    con = get_con()
    resolution = max(min(int(6 + (bbox.z[0] - 10) * (5/9)), 11), 0)
    # resolution = 7
    query = f"""    
    WITH h3_cte AS (
        SELECT
            
            ST_Y(ST_Centroid(ST_GeomFromText(geometry))) AS latitude,
            ST_X(ST_Centroid(ST_GeomFromText(geometry))) AS longitude,
            h3_latlng_to_cell(latitude, longitude, {resolution}) AS cell_id,
            subtype as name
        FROM buildings_df 
        where
            subtype IS NOT NULL
            --and subtype = 'golf'
           
            
    ),
    name_counts AS (
        SELECT
            cell_id,
            name,
            COUNT(*) AS name_count
        FROM h3_cte
        GROUP BY cell_id, name
    ),
    most_frequent_names AS (
        SELECT
            cell_id,
            name,
            name_count AS cnt
        FROM name_counts
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cell_id ORDER BY name_count DESC) = 1
    )
    SELECT 
        h3_h3_to_string(cell_id) AS cell_id,
        name AS subtype,
        cnt
    FROM most_frequent_names
    order by cnt desc;

    """
    df = con.sql(query).df()
    df = add_rgb_cmap(gdf=df, key_field="subtype", cmap_dict=CMAP)
    print(df)
    return df