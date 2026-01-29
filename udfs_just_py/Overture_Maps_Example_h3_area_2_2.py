@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-03-12-alpha-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = None, polygon: str = None, point_convert: str = None,
       resolution: int=9):
    from utils import get_overture, get_con, add_rgb_to_df
    from shapely import wkt
    import pandas as pd
    import duckdb
    import geopandas as gpd
    import shapely
    
    gdf = get_overture(bbox=bbox, release=release, theme=theme, overture_type=overture_type, use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    print(bbox)
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    buildings_df = pd.DataFrame(gdf)
    con = get_con()
    buildings_query = """    
    
       
    WITH geometry_cte AS (
        SELECT
            id,
            ST_Y(ST_Centroid(ST_GeomFromText(geometry))) AS latitude,
                ST_X(ST_Centroid(ST_GeomFromText(geometry))) AS longitude,
            ST_Transform(ST_GeomFromText(geometry), 'EPSG:4326', 'EPSG:3857', always_xy := true) AS reprojected_geometry
         FROM buildings_df
    ),

    building_area_cte as (
        SELECT
            id,
            ST_Area(reprojected_geometry) AS building_area
        FROM geometry_cte

    ),

    h3_cells_cte AS (
        SELECT
            id,
            h3_latlng_to_cell(latitude, longitude, $resolution) AS cell_id
        FROM geometry_cte
    ),

    buildings_with_h3_cte AS (
        SELECT
            ba.id,
            ba.building_area,
            h.cell_id
        FROM h3_cells_cte h
        JOIN building_area_cte ba
        ON h.id = ba.id
    ),

    h3_agg_cte AS (
        SELECT
            cell_id,
            ROUND(SUM(building_area), 2) AS tba
        FROM buildings_with_h3_cte
        GROUP BY cell_id
    )

    SELECT
        h3_h3_to_string(cell_id) AS cell_id,
    --    h3_cell_to_boundary_wkt(cell_id) AS cell_boundary, -- in the Kepler web app you don't need the geometry, it recognizes the H3 string automatically
        tba
    FROM h3_agg_cte
        
        
            """ 
    df = con.sql(buildings_query, params={'resolution': resolution}).df()
    df = add_rgb_to_df(df, 'tba')  
    print(df)
    
    return df

    