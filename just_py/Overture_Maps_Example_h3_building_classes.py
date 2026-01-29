@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-03-12-alpha-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = None, polygon: str = None, point_convert: str = None, building_class: str = None,):
    from utils import get_overture, get_con, add_rgb_cmap, CMAP
    from shapely import wkt
    import pandas as pd
    import duckdb
    import geopandas as gpd
    import shapely
    print(building_class)
    resolution = max(min(int(6 + (bbox.z[0] - 10) * (5/9)), 11), 0)
    resolution = 9
    print(f"resolution:{resolution}")
    # print(f"zoom: {bbox.z[0]}")
    gdf = get_overture(bbox=bbox, release=release, theme=theme, overture_type=overture_type, use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    buildings_df = pd.DataFrame(gdf)
    con = get_con()
    # print(buildings_df.columns.tolist())
    query = f"""    
    
        WITH latlang AS (
            SELECT       
                ST_Y(ST_Centroid(ST_GeomFromText(geometry))) AS latitude,
                ST_X(ST_Centroid(ST_GeomFromText(geometry))) AS longitude,
                class
            FROM buildings_df
            WHERE class = $building_class
            
        ), to_cells as(
            SELECT
                h3_h3_to_string(h3_latlng_to_cell(latitude, longitude, $resolution)) AS cell_id,
                class,
                COUNT(1) AS cnt
            FROM latlang
            GROUP BY 1, 2
            ) select
                cell_id,
                class,
                SUM(cnt) as cnt
                from to_cells
                group by cell_id, class
        
        
            """ 
    df = con.sql(query, params={'building_class': building_class, 'resolution': resolution}).df()
    
    # Apply color mapping directly to the DataFrame
    df = add_rgb_cmap(gdf=df, key_field="class", cmap_dict=CMAP)
    
    # Only print what we need
    # columns_to_print = ['cell_id', 'cnt']
    
    print(df)
    return df

    