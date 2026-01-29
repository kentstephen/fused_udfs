@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
       resolution: int= 15):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb

    gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=0)
    if gdf is None or gdf.empty:
        return pd.DataFrame()

    # Convert geometry to WKT using Shapely
    gdf['geometry'] = gdf['geometry'].apply(lambda x: shapely.wkt.dumps(x))
    df_buildings = pd.DataFrame(gdf)
    
    def run_query(resolution, df_buildings):
        con = fused.utils.common.duckdb_connect()
        query = f"""
        
        with to_cells as(SELECT 
            h3_polygon_wkt_to_cells(geometry, {resolution}) AS cell_id,
            COALESCE(height, 1) as height
        FROM df_buildings
        )
        SELECT unnest(cell_id) as hex,
        AVG(height) as height from to_cells
        GROUP BY cell_id
        """
        return con.sql(query).df()

    df = run_query(resolution, df_buildings)
    # print(df["height"].describe())
    # df = add_rgb(df, 'height')
    print(df)
    return df