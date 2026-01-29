@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int = 14,
        disk_size: int = 0):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb, CMAP
    gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox,overture_type="infrastructure", min_zoom=0)
    if gdf is None or gdf.empty:
        return pd.DataFrame()

    # Convert geometry to WKT using Shapely
    gdf['geometry'] = gdf['geometry'].apply(lambda x: shapely.wkt.dumps(x))
    df_buildings = pd.DataFrame(gdf)
    
    def run_query(resolution, df_buildings):
        con = fused.utils.common.duckdb_connect()
        query = f"""
        with to_cells as (
        select 
            unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS cell_id,
            subtype
        from df_buildings
        where subtype = 'bridge'
        )
        select 
            unnest(h3_grid_disk(cell_id, {disk_size})) as cell_id,
            subtype 
        from to_cells
        group by cell_id, subtype
        """
        return con.sql(query).df()

    df = run_query(resolution, df_buildings)
    if df is None or df.empty:
        return pd.DataFrame()
    df = add_rgb(gdf=df, cmap=CMAP, attr='subtype')
    print(df)
    return df
@fused.cache
def get_california_boundary():
    """Load and dissolve California census tracts into single boundary."""
    import geopandas as gpd

    print("Loading California tracts...")
    gdf = gpd.read_file(
        "https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/06_CALIFORNIA/06/tl_rd22_06_tract.zip"
    )
    gdf = gdf.dissolve().to_crs(4326)
    gdf.geometry = gdf.geometry.simplify(0.01)  # simplify for speed
    print("California boundary created")
    return gdf
