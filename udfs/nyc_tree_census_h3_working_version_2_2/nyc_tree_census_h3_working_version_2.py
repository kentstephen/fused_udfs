@fused.udf
def udf(bbox: fused.types.TileGDF=None, path: str = "s3://fused-users/stephenkentdata/nyc_tree_census.parquet"):
    # resolution = max(min(int(6 + (bbox.z[0] - 10) * (5/9)), 11), 6)
    resolution = 10
 


    print(f"resolution is {resolution}")   
    print(f'zoom is {bbox.z[0]}')
    import duckdb
    from utils import get_con, add_rgb_cmap, CMAP, get_data
    con = get_con()
    # If not loading from a parquet file,
    # swap path for tree_df in the SQl
    # and uncomment the line below

    # tree_df = get_data()
    
    query = f"""    
WITH geometry_cte AS (
    SELECT
        name,
        latitude,
        longitude
    FROM read_parquet('{path}')
    WHERE name IS NOT NULL
),
h3_cells_cte AS (
    SELECT
        name,
        h3_latlng_to_cell(latitude, longitude, {resolution}) AS cell_id
    FROM geometry_cte
),
name_counts AS (
    SELECT
        h3_h3_to_string(cell_id) AS cell_id,
        name,
        COUNT(*) AS name_count
    FROM h3_cells_cte
    GROUP BY cell_id, name
),
most_frequent_names AS (
    SELECT
        cell_id,
        name,
        name_count AS cnt
    FROM (
        SELECT
            cell_id,
            name,
            name_count,
            ROW_NUMBER() OVER (PARTITION BY cell_id ORDER BY name_count DESC) AS row_num
        FROM name_counts
    ) ranked_names
    WHERE row_num = 1
)
SELECT 
    cell_id,
    name AS tree_species,
    cnt
FROM most_frequent_names;

    """

    # Execute the query and get the DataFrame
    df = con.sql(query).df()
    
    # Apply color mapping directly to the DataFrame
    df = add_rgb_cmap(gdf=df, key_field="tree_species", cmap_dict=CMAP)
    
    # Assume df is your DataFrame
    columns_to_print = ['cell_id', 'tree_species', 'cnt']
    print(df[columns_to_print])
    # print(df)
    return df
