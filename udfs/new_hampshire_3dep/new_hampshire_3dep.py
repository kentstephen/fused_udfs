common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
@fused.udf()
def udf(
    bounds: fused.types.Bounds= None,
    collection="3dep-seamless",
    band="data",
    res_factor:int=4,
    res:int=9
):
    
    # convert bounds to tile
    tile = common.get_tiles(bounds, clip=True)
    # df = run_query(bounds, res)
    # return df
    #initial parameters
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    try:
        df = run_query(bounds=bounds, res=res)
    except Exception as e:
        return None
    if df is None or df.empty:
        return None
    df["elev_scale"] = int((15 - z) * 1)
    print(df)
    return df

@fused.cache
def get_boundary(bounds,res):
    
    import shapely 
    import pandas as pd
    gdf = fused.run('fsh_6EKjYUqUAwM4Nia0UCtUfC', bounds=bounds)
    gdf = gdf.dissolve()
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    # 
    df = pd.DataFrame(gdf)
    bounds = common.bounds_to_gdf(bounds) 
    bounds = bounds.bounds.values[0]
    xmin, ymin, xmax, ymax = bounds
    utils = fused.load(
    "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    con = utils.duckdb_connect()
    query = f"""
    with boundary as (
      select
    unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'center')) as boundary_hex
    from df
    )
    select boundary_hex from boundary
        where
     h3_cell_to_lat(boundary_hex) >= {ymin}
    AND h3_cell_to_lat(boundary_hex) < {ymax}
    AND h3_cell_to_lng(boundary_hex) >= {xmin}
    AND h3_cell_to_lng(boundary_hex) < {xmax}

  
    """
    return con.sql(query).df()

def run_query(bounds, res):
    df_dem = fused.run('fsh_20qdh0kfvTqqcuvQeDW7og', bounds=bounds, res=res)
    df_boundary = get_boundary(bounds,res)
    print(df_boundary)
    
    utils = fused.load(
    "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    con = utils.duckdb_connect()
 
    query = f"""

    

    select
    d.hex,
    d.metric
    from df_dem d inner join df_boundary b on d.hex=b.boundary_hex
    --group by 1
    """
    return con.sql(query).df()
        