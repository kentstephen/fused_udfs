@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=14):
    import geopandas as gpd
    from utils import add_rgb
    import shapely
    import pandas as pd
    import math
    gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=0)
    if gdf is None or gdf.empty:
        return pd.DataFrame()

    # Convert geometry to WKT using Shapely
    gdf['geometry'] = gdf['geometry'].apply(lambda x: shapely.wkt.dumps(x))
    df_buildings = pd.DataFrame(gdf)
    con = fused.utils.common.duckdb_connect()
    def run_query(resolution, df_buildings):
        query = f"""
        
        SELECT 
            h3_latlng_to_cell(ST_Y(ST_Centroid(ST_GeomFromText(geometry))), ST_X(ST_Centroid(ST_GeomFromText(geometry))), {resolution}) AS cell_id,
            AVG(COALESCE(height, 1)) as avg_height
        FROM df_buildings
        GROUP BY cell_id
        """
        return con.sql(query).df()

    df = run_query(resolution, df_buildings)
   
    df = add_rgb(df, 'avg_height')
    print(df)
    return df