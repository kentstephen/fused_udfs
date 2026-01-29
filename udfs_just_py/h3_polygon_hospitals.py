@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int = 14,
        disk_size: int = 0):
    res_offset = 0  # lower makes the hex finer
    # resolution = max(min(int(4 + bbox.z[0] / 1.4), 15) - res_offset, 2)
    print(bbox.z[0])
    print(resolution)
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb, CMAP
    # get_overture():
    gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=0)
    if gdf is None or gdf.empty:
        return pd.DataFrame()
    gdf = gdf[gdf['class'] == 'hospital']
    if gdf is None or gdf.empty:
        return pd.DataFrame()
    gdf['geometry'] = gdf['geometry'].apply(lambda x: shapely.wkt.dumps(x))
    
    
        
    print(gdf)

    # gdf = get_overture()
    df_buildings = pd.DataFrame(gdf)
    
    def run_query(resolution, df_buildings):
        con = fused.utils.common.duckdb_connect()
        query = f"""
        with to_cells as (
        select 
            unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS cell_id,
            subtype
        from df_buildings
        
        )
        select 
            unnest(h3_grid_disk(cell_id, {disk_size})) as cell_id,
            subtype 
        from to_cells
        group by cell_id, subtype
        """
        return con.sql(query).df()

    df = run_query(resolution, df_buildings)
    print(df)
    if df is None or df.empty:
        return pd.DataFrame()
    # df = add_rgb(gdf=df, cmap=CMAP, attr='subtype')
    print(df)
    return df