@fused.udf
def udf(bbox = (-72.818472, 44.072055, -72.299101, 44.41482), 
        resolution = 10):
    import geopandas as gpd
    import shapely
    import duckdb
    from shapely import wkt 
    import fused 
    from utils import get_overture, get_water, get_con, buildings_query, final_query
    import geopandas as gpd
    from shapely.geometry import box

    
    buildings_df = get_overture(bbox=bbox)
    water_df = get_water(bbox=bbox)
    
    con = get_con()
    
    con.sql(buildings_query, params={'resolution': resolution})
    
    df = con.sql(final_query).df()
    print(df)
    return df