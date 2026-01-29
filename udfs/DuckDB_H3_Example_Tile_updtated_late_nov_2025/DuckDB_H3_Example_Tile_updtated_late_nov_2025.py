@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds = None, resolution: int = 14, min_count: int = 1):
    import shapely
    import geopandas as gpd

    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    con = common.duckdb_connect()
    
    def read_data(bounds, resolution):
        xmin, ymin, xmax, ymax = bounds
        df_over_h3 = fused.run("fsh_7EiRMtV3XKj72UYGqS0Psg", bounds=bounds, resolution=resolution)
        query = f"""
    
    
        WITH parquet_hexes AS (
    SELECT h3_latlng_to_cell(dropoff_latitude, dropoff_longitude, {resolution}) as hex
    FROM read_parquet('s3://fused-asset/misc/nyc/tlc/trip-data/yellow_tripdata_2010-01.parquet')
    )
    SELECT p.hex, count(1) as cnt
    FROM parquet_hexes p
    LEFT JOIN df_over_h3 o ON p.hex = o.hex
    WHERE o.hex IS NULL
    AND h3_cell_to_lat(p.hex) >= {ymin}
            AND h3_cell_to_lat(p.hex) < {ymax}
               AND h3_cell_to_lng(p.hex) >= {xmin}
               AND h3_cell_to_lng(p.hex) < {xmax}
        GROUP BY 1
        HAVING cnt>{min_count}
        """
        # print(query)
    
        return con.sql(query).df()
    df = read_data(bounds, resolution)
    df = df[df['cnt'] < 600]
    # print("number of trips:", df.cnt.sum())
    print(df)
    return df
    # gdf = gpd.GeoDataFrame(df.drop(columns=["boundary"]), geometry=df.boundary.apply(shapely.wkt.loads))
    # print(gdf)
    # return gdf
