@fused.udf 
def udf(
    bbox=None,
    resolution: int = 10,
    date_1: str = '2012-10-26',
    date_2: str = '2012-11-15',
 
):
    from utils import get_overture, get_data, add_rgb
    import duckdb
    import geopandas as gpd
    import pandas as pd
    import shapely
    time_chunks = {
    "early_morning": "incident_time >= '00:00:00' AND incident_time < '05:00:00'",
    "morning": "incident_time >= '05:00:00' AND incident_time < '10:00:00'",
    "late_morning": "incident_time >= '10:00:00' AND incident_time < '12:00:00'",
    "afternoon": "incident_time >= '12:00:00' AND incident_time < '17:00:00'",
    "evening": "incident_time >= '17:00:00' AND incident_time < '21:00:00'",
    "night": "incident_time >= '21:00:00' AND incident_time <= '24:60:59'"
}

# Example usage in a query
    selected_time_chunk = time_chunks["night"]

    df_calls = get_data()
    h3_utils = fused.load(
        "https://github.com/fusedio/udfs/tree/870e162/public/DuckDB_H3_Example/"
    ).utils
    # print(df_calls['incident_time'])
    
    # Create DuckDB connection
    con = duckdb.connect()
    # con.sql(f"""INSTALL httpfs; LOAD httpfs;""")
    h3_utils.load_h3_duckdb(con)
    query = f"""
WITH h3_cte AS (
    SELECT
        descriptor AS name,
        DATE_TRUNC('day', CAST(created_date AS DATE)) AS date,
        h3_latlng_to_cell(latitude, longitude, {resolution}) AS cell_id,
        COUNT(1) AS cnt
    FROM df_calls
    WHERE DATE_TRUNC('day', CAST(created_date AS DATE)) 
        BETWEEN CAST('{date_1}' AS DATE) AND CAST('{date_2}' AS DATE)
        AND latitude IS NOT NULL AND longitude IS NOT NULL
    GROUP BY 1, 2, 3  -- Group by descriptor, date, and cell_id
    ORDER BY cnt DESC
),
name_counts AS (
    SELECT
        h.cell_id,
        h.name,
        SUM(h.cnt) AS name_count  -- Sum the counts per descriptor for each cell_id
    FROM h3_cte h
    GROUP BY h.cell_id, h.name
),
most_frequent_names AS (
    SELECT
        nc.cell_id,
        nc.name,
        nc.name_count,
        ROW_NUMBER() OVER (PARTITION BY nc.cell_id ORDER BY nc.name_count DESC) AS rn  -- Window function to rank the most frequent name
    FROM name_counts nc
)
SELECT 
    h3_h3_to_string(mfn.cell_id) AS cell_id,
    h.date,
    mfn.name AS descriptor,
    h.cnt
    
FROM most_frequent_names mfn
JOIN h3_cte h
    ON mfn.cell_id = h.cell_id
    AND mfn.name = h.name
WHERE mfn.rn = 1;


    
    """
    df = con.sql(query).df()
    # print(df['incident_time'])
    
    # gdf_calls = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    # gdf_overture = get_overture(
    #     bbox=bbox,
    #     release=release,
    #     theme=theme,
    #     overture_type=overture_type,
    #     use_columns=use_columns,
    #     num_parts=num_parts,
    #     min_zoom=min_zoom,
    #     polygon=polygon,
    #     point_convert=point_convert
    # )

    # if gdf_overture is not None and not gdf_overture.empty:
    #     gdf_joined = gdf_overture.sjoin(gdf_calls)
    #     gdf_joined = add_rgb(gdf_joined, 'cnt') 
    #     print(gdf_joined)
    #     return gdf_joined
    # else:
    #     # Return an empty GeoDataFrame with the same structure as gdf_overture
    #     gdf_joined = gpd.GeoDataFrame(columns=gdf_overture.columns if gdf_overture is not None else [])
    #     print("No data in gdf_overture")
    # df = add_rgb(df, 'cnt') 
    # print(df)
    print(df)
    return df