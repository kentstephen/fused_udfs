@fused.cache
def read_data():
    import pandas as pd
    return pd.read_csv('https://data.cityofnewyork.us/api/views/uvpi-gqnh/rows.csv?accessType=DOWNLOAD&api_foundry=true')

def df_to_hex(df, df_dem, res):
    # xmin, ymin, xmax, ymax = bounds
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""with to_cells as (
            SELECT h3_latlng_to_cell(latitude, longitude, {res}) AS hex, 
            string_agg(spc_common) as spc_agg
            FROM df

            group by 1)
            select
            t.hex,
            d.ndvi,
            d.canopy_height,
            d.total_elevation,
            t.spc_agg
            from to_cells t left join df_dem d on t.hex=d.hex
            
            
          -- order by 1
        """
    con = utils.duckdb_connect()
    return con.query(qr).df()