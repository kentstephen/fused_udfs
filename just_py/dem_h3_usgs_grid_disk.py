utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection="3dep-seamless",
    band="data",
    res_factor:int=15,
    res:int=12
):
    df_dem = fused.run('fsh_5oDZgepOmwcmW5Ira4HM1T', bounds=bounds, res=res) # dem_10meter_tile_hex_3
    bounds = utils.bounds_to_gdf(bounds) 
    bounds = bounds.bounds.values[0]

    df = add_grid(df_dem, bounds, res)
    print(df)
    return df

def add_grid(df_dem, bounds, res):
    xmin, ymin, xmax, ymax = bounds
    con = utils.duckdb_connect()
    query = f"""

with to_disk as (
    select 
        unnest(h3_grid_disk(hex, 1)) as hex,
        metric
    from df_dem
    )
    select 
        hex,
        round(avg(metric), 2) as metric
    from to_disk
                where
    h3_cell_to_lat(hex) >= {ymin}
    AND h3_cell_to_lat(hex) < {ymax}
    AND h3_cell_to_lng(hex) >= {xmin}
    AND h3_cell_to_lng(hex) < {xmax}
    group by 1
    
    """
    return con.sql(query).df()