def df_to_hex(df, res,bounds_values, latlng_cols=("lat", "lng")):
    xmin, ymin, xmax, ymax = bounds_values
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            sum(area_m2) as cnt,
            sum(height) as height
            FROM df
                    where
    h3_cell_to_lat(hex) >= {ymin}
    AND h3_cell_to_lat(hex) < {ymax}
    AND h3_cell_to_lng(hex) >= {xmin}
    AND h3_cell_to_lng(hex) < {xmax}
            group by 1
          -- order by 1
        """
    con = utils.duckdb_connect()
    return con.query(qr).df()