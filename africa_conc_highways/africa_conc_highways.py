import fused 
@fused.udf

def udf(bbox=None, resolution=7):
    import duckdb
    import pandas as pd
    import logging
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from threading import Lock
    set_option('request_timeout', 300) 
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Function to create a new DuckDB connection
    def create_connection():
        con = duckdb.connect()
        con.sql("""
            INSTALL h3 FROM community;
            LOAD h3;
            INSTALL spatial;
            LOAD spatial;
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_region='us-west-2';
        """)
        return con
    
    # Function to execute the query and return the result as a DataFrame
    def query_and_insert(country_code, index, total_countries, progress_lock, progress):
            
        con = create_connection()
        df = pd.DataFrame()  # Initialize an empty DataFrame
    
        try:
            results = con.sql(f"""
            SELECT
                bbox,
                ST_AsText(ST_GeomFromWKB(geometry)) AS wkt
            FROM read_parquet('s3://overturemaps-us-west-2/release/2024-06-13-beta.0/theme=divisions/type=division_area/*', filename=true, hive_partitioning=1)
            WHERE
                subtype = 'country'
                AND country = '{country_code}'
            """).fetchall()
    
            if results:
                bbox, wkt_geom = results[0]
    
                query = f"""
         WITH geometry_cte AS (
            SELECT 
                ST_GeomFromWKB(geometry) AS geom,
                CAST(UNNEST(generate_series(1, ST_NPoints(ST_GeomFromWKB(geometry)))) AS INTEGER) AS point_index
            FROM read_parquet('s3://overturemaps-us-west-2/release/2024-04-16-beta.0/theme=transportation/type=segment/*', filename=true, hive_partitioning=1)
            WHERE
                bbox.xmin <= {bbox["xmax"]}
                AND bbox.xmax >= {bbox["xmin"]}
                AND bbox.ymin <= {bbox["ymax"]}
                AND bbox.ymax >= {bbox["ymin"]}
                AND ST_Intersects(ST_GeomFromWKB(geometry), ST_GeomFromText('{wkt_geom}'))
                AND class = 'motorway'
            ),
            points_cte AS (
                SELECT
                    ST_PointN(geom, point_index) AS point
                FROM geometry_cte
            ),
            h3_cells AS (
                SELECT
                    h3_h3_to_string(h3_latlng_to_cell(ST_Y(point), ST_X(point), {resolution})) AS cell_id,
                    COUNT(1) as cnt
                FROM points_cte
                GROUP BY h3_h3_to_string(h3_latlng_to_cell(ST_Y(point), ST_X(point), {resolution}))
            ),
            aggregated_cells AS (
                SELECT
                    cell_id,
                    SUM(cnt) AS total_cnt
                FROM h3_cells
                GROUP BY cell_id
            )
            SELECT cell_id, total_cnt FROM aggregated_cells;
    
                """
                df = con.execute(query).df()
            else:
                logging.info(f"No results found for country code: {country_code}")
    
        except Exception as e:
            logging.error(f"Error processing country code {country_code}: {e}")
    
        finally:
            con.close()
    
        with progress_lock:
            progress[0] += 1
            print(f"{progress[0]} out of {total_countries} countries processed.")
    
        return df
    
    # List of two-letter ISO country codes for African countries
    african_country_codes_2letter = [
        'DZ', 'AO', 'BJ', 'BW', 'BF', 'BI', 'CV', 'CM', 'CF', 'TD', 'KM', 'CG', 'CD', 'DJ', 'EG', 'GQ', 'ER', 'SZ', 'ET', 'GA', 'GM', 'GH', 'GN', 'GW', 'CI', 'KE', 'LS', 'LR', 'LY', 'MW', 'ML', 'MR', 'MU', 'YT', 'MA', 'MZ', 'NA', 'NE', 'NG', 'RE', 'RW', 'SH', 'ST', 'SN', 'SC', 'SL', 'SO', 'ZA', 'SS', 'SD', 'TZ', 'TG', 'MG', 'TN', 'UG', 'ZM', 'ZW'
    ]
    
    
    # Run the queries concurrently and collect the DataFrames
    dfs = []
    total_countries = len(african_country_codes_2letter)
    progress = [0]
    progress_lock = Lock()
    
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(query_and_insert, code, index, total_countries, progress_lock, progress) for index, code in enumerate(african_country_codes_2letter)]
    
    # Wait for all threads to complete and collect the results
    for future in as_completed(futures):
        dfs.append(future.result())
    
    # Concatenate all DataFrames
    final_df = pd.concat(dfs, ignore_index=True)
    
    # Save the final DataFrame to a CSV file
    # final_df.to_csv('africa_highways_r_7.csv', index=False)
    print(final_df)
    return final_df