@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10, path:str="s3://fused-users/stephenkentdata/hospitals_SCOTLAND_r_8_iso.parquet"):
  
    import geopandas as gpd
    import shapely
    import duckdb
    import pandas as pd
    import polars as pl
    from utils import add_rgb, get_isochrone_cells
    # wkt_geom = get_texas_boundary_wkt()
    bounds = bbox.bounds.values[0]
    @fused.cache
    def read_data(path):
       return duckdb.sql(f"from read_parquet('{path}')").df()
        
    # df_iso = pl.read_parquet('s3://fused-users/stephenkentdata/hospitals_NEW_ENG_r_8_iso.parquet')
    df_iso_stg= read_data(path)
    df_iso = get_isochrone_cells(bounds, df_iso_stg)
    print(df_iso)
    @fused.cache
    def get_kontur_us(bbox):
        df_kontur_us = fused.run("fsh_1Kg290kb0BPNahbrby4Kah", bbox=bbox)
        # df_kontur_ca = fused.run("fsh_5Y7zaCy0KYVPjrBIZ42NNK", bbox=bbox)
        return df_kontur_us
    @fused.cache
    def get_kontur_ca(bbox):
        df_kontur_ca = fused.run("fsh_5Y7zaCy0KYVPjrBIZ42NNK", bbox=bbox)
        return df_kontur_ca

    df_kontur_us = get_kontur_us(bbox)         
    df_kontur_ca = get_kontur_ca(bbox) 
    # Dictionary of US and CA dfs
      # Run the query with both inputs

    # df_kontur_us = fused.run("fsh_1Kg290kb0BPNahbrby4Kah", bbox=bbox)
    # df_kontur_ca = fused.run("fsh_5Y7zaCy0KYVPjrBIZ42NNK", bbox=bbox)
   
    # df_kontur = get_kontur(bbox)
    # mask_wkt = get_mask_wkt()
    @fused.cache
    def run_query(df_iso, df_kontur_us, df_kontur_ca):
        con = fused.utils.common.duckdb_connect()
        query = """
        select
            i.hex,
            i.cnt as cnt,
            COALESCE(k_us.pop, 0) + COALESCE(k_ca.pop, 0) as pop
        from df_iso i
        left join df_kontur_us k_us on i.hex = k_us.hex
        left join df_kontur_ca k_ca on i.hex = k_ca.hex
        """
        return con.sql(query).df()
    df = run_query(df_iso, df_kontur_us, df_kontur_ca)
    # print(df)
    # df = add_rgb(df, 'cnt')
    return df

  