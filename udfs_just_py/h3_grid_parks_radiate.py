@fused.udf
def udf(bbox: fused.types.TileGDF, resolution: int=12):
    import geopandas as gpd
    import shapely
    from utils import add_rgb

    # @fused.cache
    # def get_gdf():
    #     import pandas as pd
    #     from shapely import wkt
    #     import geopandas as gpd
    #     buffer_distance = 40
    #     df = pd.read_csv('https://data.cityofnewyork.us/api/views/esmy-s8q5/rows.csv?accessType=DOWNLOAD&api_foundry=true')
    #     df["geometry"] = df["the_geom"]
    #     df['geometry'] = df['geometry'].apply(wkt.loads)
    #     gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    #     gdf = gdf.to_crs("EPSG:4326")
    
    #     return gdf
    # gdf = get_gdf()
    # return gdf
    gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, overture_type='land_use', min_zoom=0)
    if len(gdf) < 1:
        return
    gdf = gdf[gdf['subtype'] == 'park']
    # return gdf
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)

    def get_cells(resolution, gdf):
        con = fused.utils.common.duckdb_connect()
        query = f"""
  with to_cells as (
    select
        cast(unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS UBIGINT) as hex,
        id
    from gdf

), 
distances as (
    select
        a.id,
        a.hex,
        least(max(h3_grid_distance(a.hex, b.hex)), 50) as max_distance,
        count(*) as cell_count  -- Number of cells in original park
    from to_cells a 
    join to_cells b on a.id = b.id
    group by 1, 2
),
to_disk as (
    select 
        d.id,
        unnest(h3_grid_disk(d.hex, CAST(d.max_distance AS INTEGER))) as disk_hex,
        cell_count / (max_distance + 1) as influence_score  -- Decreases with distance from center
    from distances d
)
select 
    disk_hex as hex,
  --  h3_cells_to_multi_polygon_wkt(array_agg(disk_hex)),
--h3_cell_to_boundary_wkt(hex),
    
    sum(influence_score) as cnt
from to_disk 
group by 1;      """        
        return con.sql(query).df()
    # df = get_cells(resolution, gdf)
    # gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    df_final = get_cells(resolution, gdf)
    df_final = add_rgb(df_final, 'cnt')
    return df_final
    