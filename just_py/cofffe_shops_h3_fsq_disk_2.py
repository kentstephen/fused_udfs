@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=11, disk_size: int = 10):
    import geopandas as gpd
    import shapely
    from utils import add_rgb

    df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    if len(df) < 1:
        return
    df = df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
    # df_metro = fused.run("fsh_5z2L8DVfwXYBDihSf47onN", bbox=bbox, resolution=resolution)
    
    df['geometry'] = df['geometry'].apply(shapely.wkt.dumps)

    def get_cells(resolution, df):
        con = fused.utils.common.duckdb_connect()
        query = f"""
       with to_cells as (
       select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), 9) as hex,
        count(1) as cnt
        from df
        group by 1
    ),
  to_disk as (
    select 
        unnest(h3_grid_disk(hex, {disk_size})) as disk_hex,
        cnt
    from to_cells
)
select 
   disk_hex as hex,
    SUM(coalesce(cnt, 0)) as cnt
from to_disk 
group by 1
        """        
        return con.sql(query).df()

    df_final = get_cells(resolution, df)
    df_final = add_rgb(df_final, 'cnt')
    return df_final