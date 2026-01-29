common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
@fused.udf()
def udf(
    bounds: fused.types.Bounds= None,
    collection="3dep-seamless",
    band="data",
    res_factor:int=4,
    res:int=10
):
    
        
    import shapely 
    import pandas as pd
    gdf = fused.run('fsh_6EKjYUqUAwM4Nia0UCtUfC', bounds=bounds)
    gdf = gdf.dissolve()
 
    bounds_gdf = common.bounds_to_gdf(bounds)
    # gdf = gdf.sjoin(bounds_gdf)
    # bounds_values = bounds_gdf.bounds.values[0]
    # xmin, ymin, xmax, ymax = bounds_values

    minx, miny, maxx, maxy = bounds_gdf.total_bounds
    gdf = gdf.cx[minx:maxx, miny:maxy]
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    # 
    df = pd.DataFrame(gdf)
    if df is None or df.empty:
        return
    utils = fused.load(
    "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    con = utils.duckdb_connect()
    query = f"""
   -- with boundary as (
      select
    unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'center')) as boundary_hex
    from df


  
    """
    return con.sql(query).df()