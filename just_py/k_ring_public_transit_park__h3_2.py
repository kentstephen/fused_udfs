@fused.udf
def udf(bbox: fused.types.TileGDF=None,
       resolution = 11,
       k_ring = 15):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import get_con
    from shapely import wkt
  
    overture_udf = fused.load("stephen.kent.data@gmail.com/Overture_Maps_Example")
    gdf_overture = fused.run(
        overture_udf,
        release="2024-09-18-0",
        theme="places",
        overture_type="place",
        bbox=bbox,
        engine='local',
        min_zoom=0)
    gdf_overture['geometry'] = gdf_overture['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    places_df = pd.DataFrame(gdf_overture)
    # print(places_df.dtypes)
    # print(places_df["categories"])
    # print(gdf_overture)
    con = get_con()
    buildings_query = """    
    
       
    WITH places_cte AS (
        SELECT
            h3_h3_to_string(h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), $resolution)) AS cell_id,
            COUNT(1) AS cnt
        FROM places_df 
       where --categories.primary LIKE '%transit%' 
         categories.primary LIKE '%subway%'
        OR categories.primary LIKE '%bus%'
        
         GROUP BY cell_id
    ),
    hospital_cte as(
    SELECT
        h3_h3_to_string(h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), $resolution)) AS cell_id
        from places_df
        where categories.primary = 'park'
    ),
   
expanded_disk AS (
  SELECT unnest(h3_grid_disk(cell_id, $k_ring)) AS h3_cell_expanded
  FROM hospital_cte
)

SELECT p.cell_id AS cell_id, SUM(p.cnt) as cnt
FROM places_cte p
JOIN expanded_disk d
ON p.cell_id = d.h3_cell_expanded 
group by cell_id
    

        
        
            """ 


    df = con.sql(buildings_query, params={'resolution': resolution, 'k_ring':k_ring}).df()
     
    print(df)
    
    return df