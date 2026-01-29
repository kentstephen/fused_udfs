@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-03-12-alpha-0", theme: str = "buildings", overture_type: str = "building", use_columns: list = None, num_parts: int = None, min_zoom: int = None, polygon: str = None, point_convert: str = None,
       resolution: int = 11):
    from utils import get_overture, add_rgb_to_df
    from shapely import wkt
    import pandas as pd
    import duckdb
    import geopandas as gpd
    import shapely
    
    gdf = get_overture(bbox=bbox, release=release, theme=theme, overture_type=overture_type, use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    print(bbox)
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    buildings_df = pd.DataFrame(gdf)
    duckdb_connect = fused.load(
            "https://github.com/fusedio/udfs/tree/a1c01c6/public/common/"
        ).utils.duckdb_connect
    con = duckdb_connect()
    buildings_query = """    
    
SELECT
    h3_h3_to_string(h3_latlng_to_cell(ST_Y(ST_Centroid(ST_GeomFromText(geometry))), ST_X(ST_Centroid(ST_GeomFromText(geometry))), $resolution)) AS cell_id,
    h3_cell_to_boundary_wkt(cell_id) as boundary,
    ROUND(SUM(ST_Area(ST_Transform(ST_GeomFromText(geometry), 'EPSG:4326', 'EPSG:3857', always_xy := true))), 2) AS tba
FROM buildings_df
GROUP BY cell_id;
        
        
            """ 
    df = con.sql(buildings_query, params={'resolution': resolution}).df()
    df = add_rgb_to_df(df, 'tba')  
    print(df)
    
    return df

    