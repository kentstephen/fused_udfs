@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-08-20-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = None, polygon: str = None, point_convert: str = None,
       resolution: int=9, k_ring: int=25):
    from utils import get_overture, get_con, add_rgb_to_df
    import geopandas as gpd
    import pandas as pd
    from shapely import wkt
    hospital_gdf = get_overture(bbox=bbox, release=release, theme="places", overture_type="place", use_columns=use_columns, num_parts=num_parts, min_zoom=0, polygon=polygon, point_convert=point_convert)
    print(hospital_gdf.columns)
    hospital_gdf['geometry'] = hospital_gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    print(hospital_gdf)
    mask = hospital_gdf['categories'].apply(lambda x: isinstance(x, dict) and x.get('primary') == "hospital")
    hospital_gdf = hospital_gdf[mask]
    hospital_df = pd.DataFrame(hospital_gdf)
    building_gdf = get_overture(bbox=bbox, release=release, theme="buildings", overture_type="building", use_columns=use_columns, num_parts=num_parts, min_zoom=0, polygon=polygon, point_convert=point_convert)
    building_gdf['geometry'] = building_gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    building_df = pd.DataFrame(building_gdf)
    print(building_df.columns)
    
    mask_gdf = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/50_VERMONT/50/tl_rd22_50_tract.zip')
    
    # Step 2: Dissolve the GeoDataFrame to create a single boundary polygon
    gdf_dissolved = mask_gdf.dissolve()
    
    # Step 3: Convert CRS to EPSG:4326 if necessary
    gdf_dissolved = gdf_dissolved.to_crs(epsg=4326)
    
    # Step 4: Convert geometry to WKT
    gdf_dissolved['geometry'] = gdf_dissolved.geometry.apply(lambda x: wkt.dumps(x))
    
    # Step 5: Create a Pandas DataFrame with the WKT geometry column named 'geometry'
    df_wkt = pd.DataFrame(gdf_dissolved[['geometry']])

    con = get_con()
    buildings_query = """    
    
       
    WITH building_cte AS (
        SELECT
            h3_latlng_to_cell(ST_Y(ST_Centroid(ST_GeomFromText(b.geometry))), ST_X(ST_Centroid(ST_GeomFromText(b.geometry))), $resolution) AS cell_id,
            COUNT(1) AS total_area
        FROM building_df b
        JOIN df_wkt g ON ST_Intersects(ST_Centroid(ST_GeomFromText(b.geometry)), ST_GeomFromText(g.geometry))
         GROUP BY cell_id
    ),
    hospitals as (

    SELECT
    h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), $resolution) AS cell_id,
FROM hospital_df

    ), 
expanded_disk AS (
  SELECT unnest(h3_grid_disk(cell_id, $k_ring)) AS h3_cell_expanded
  FROM hospitals
)

SELECT h3_h3_to_string(b.cell_id) AS cell_id, SUM(b.total_area) as total_area
FROM building_cte b
JOIN expanded_disk d
ON d.h3_cell_expanded = b.cell_id
group by cell_id
    

        
        
            """ 
    df = con.sql(buildings_query, params={'resolution': resolution, 'k_ring':k_ring}).df()
    df = add_rgb_to_df(df, 'total_area')  
    print(df)
    
    return df

    
    