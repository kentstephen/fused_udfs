@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=12):
    import geopandas as gpd
    import shapely
    from utils import add_rgb
    import pandas as pd
    gdf = fused.run("fsh_6pheiWmU58gADJE8FKP5xg")
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    df_coffee = pd.DataFrame(gdf)
    if df_coffee is None or df_coffee.empty:
        return
        
    
    def get_cells(df_coffee, resolution):
        con = fused.utils.common.duckdb_connect()
        query=f"""
       with to_cells as (
  select
    unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS hex
  from df_coffee
)
select 
  hex,
  h3_cell_to_boundary_wkt(hex) as boundary,
  count(*) as cnt
from to_cells
group by hex
        
        """
        df = con.sql(query).df()
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
        
   
    df = get_cells(df_coffee, resolution)
    print(type(df))
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=10)
    # if len(gdf_overture) < 1:
    #     return
    gdf_joined = gdf_overture.sjoin(df, how="left", predicate="intersects")
    return gdf_joined
    # print(df)
    # df = add_rgb(df, 'cnt')
    # return df