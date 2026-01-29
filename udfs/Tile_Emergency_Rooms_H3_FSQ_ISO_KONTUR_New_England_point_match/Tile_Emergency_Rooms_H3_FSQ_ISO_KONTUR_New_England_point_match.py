@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10, path:str="s3://fused-users/stephenkentdata/Emergency_rooms_point_match_new_england_r_8.parquet"):
  
    import geopandas as gpd
    import shapely
    import duckdb
    import pandas as pd
    import polars as pl
    from utils import add_rgb, get_isochrone_cells, get_mask_wkt
  
    bounds = bbox.bounds.values[0]
    # @fused.cache
    def read_data(path):
       return duckdb.sql(f"from read_parquet('{path}')").df()
        
    df_iso= read_data(path)
   
    
    # @fused.cache
    def get_kontur_us(bbox):
        df_kontur_us = fused.run("fsh_1Kg290kb0BPNahbrby4Kah", bbox=bbox)
        return df_kontur_us
    
    df_kontur_us = get_kontur_us(bbox)         

    mask_wkt = get_mask_wkt()
    # @fused.cache(reset=True)
    # @fused.cache(reset=True)
    def run_query(df_iso, df_kontur_us, mask_wkt):
        con = fused.utils.common.duckdb_connect()
        query = f"""
        with agg_cells as (
        select 
            h3_h3_to_string(coalesce(i.hex, k_us.hex)) as hex,
            CAST(list(reaching_hospital_cells) as varchar) as nearby_ERs,
            SUM(COALESCE(k_us.pop, 0)) as pop
        from df_kontur_us k_us
        full outer join df_iso i on i.hex = k_us.hex
        GROUP BY 1
       ) select * from agg_cells
       where st_intersects(st_geomfromtext(h3_cell_to_boundary_wkt(hex)), st_geomfromtext('{mask_wkt}'))
        """
        return con.sql(query).df()
    df = run_query(df_iso, df_kontur_us, mask_wkt)
    
    print(df)
    # df = add_rgb(df, 'cnt')
    return df