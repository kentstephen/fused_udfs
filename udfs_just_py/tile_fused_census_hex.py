@fused.udf
def udf(bounds: fused.types.Bounds= [-126.6,20.9,-64.9,50.4], 
        res:int=None, 
        stats_type:str="mean_w_filter",
        png:bool=False,
        color_scale:float=1
):

    import pandas as pd
    # import duckdb
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds)


    df = run_query(tile)
    return df
    
def run_query(tile):
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    # import duckdb
    # minx, miny, maxx, maxy = bounds
    xmin, ymin, xmax, ymax = tile.geometry.iloc[0].bounds
    query=f"""select * 
                from read_parquet('https://data.source.coop/fused/hex/release_2025_04_beta/census/2020_partitioned_h7.parquet')
            WHERE
        h3_cell_to_lat(hex) >= {ymin} -- make sure we don't have overlap bewtween tiles
        AND h3_cell_to_lat(hex) < {ymax}
        AND h3_cell_to_lng(hex) >= {xmin}
        AND h3_cell_to_lng(hex) < {xmax}
                """
    con = common.duckdb_connect()
    return con.sql(query).df()