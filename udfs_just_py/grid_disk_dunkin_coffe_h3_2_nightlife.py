@fused.udf
def udf(bbox: fused.types.TileGDF=None,
       resolution = 10,
       k_ring =1):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import get_con, add_rgb
    from shapely import wkt
    # resolution = max(min(int(6 + (bbox.z[0] - 10) * (5/9)), 11), 0)
    overture_udf = fused.load("<your-email>/Overture_Maps_Example")
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
    SELECT
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(St_geomFromtext(geometry)), $resolution) AS cell_id,
        count(1) as cnt
    FROM places_df
    WHERE
    
        categories.primary like '%restaurant%'
        or categories.primary like '%bar%'
        OR categories.primary like '%lounge%'
        or categories.primary like '&club&'
    GROUP BY cell_id
    

        
        
            """ 


    df = con.sql(buildings_query, params={'resolution': resolution}).df()
    df = add_rgb(df, 'cnt')  
    print(df)
    
    return df