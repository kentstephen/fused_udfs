@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int=12,
       disk_size: int=50):
    import geopandas as gpd
    import shapely
    import geopandas
    from utils import add_rgb
    df_starb = fused.run("fsh_4U4DnZPLzzKscAMoETmgnN", bbox=bbox, resolution=resolution)
    df_coffee = fused.run("fsh_EdnbhTPUWgmFOZImlwBm1", bbox=bbox, resolution=resolution)
    if len(df_starb) < 1:
        return
    if len(df_coffee) < 1:
        return
    def get_cells(resolution, df_starb, df_coffee):
        con = fused.utils.common.duckdb_connect()
        query = f"""
with to_disk as (
    select 
        unnest(h3_grid_disk(s.hex, {disk_size})) as disk_hex,
        s.hex as center_hex
    from df_starb s
)
select 
    d.center_hex as hex,
    h3_cell_to_boundary_wkt(d.center_hex) boundary,
    SUM(coalesce(c.cnt, 0)) as competition_cnt
from to_disk d 
left join df_coffee c on d.disk_hex = c.hex
group by 1;
        """        
        df = con.sql(query).df()
        gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
        return gdf

    gdf = get_cells(resolution, df_starb, df_coffee)
    # gdf = add_rgb(gdf, 'competition_cnt')
    print(gdf["competition_cnt"])
    return gdf
  