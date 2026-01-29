@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=8, date: str = '2024-01-01'):
    import geopandas as gpd
    import shapely
    import duckdb
    import pandas as pd
    from utils import get_emergency_cells, get_precincts, add_rgb
    
    # print(df_calls['create_date'])
    # df_test = duckdb.sql("select date_trunc('month', cast(create_date as timestamp))  from df_calls --WHERE date_trunc('day', cast(create_date as timestamp)) = '2024-01-11'")
    # print(df_test)
    df_stations = get_precincts()
  

    # def h3_cluster_analysis(resolution):
    df_calls = get_emergency_cells()
    print(df_calls)
    con = fused.utils.common.duckdb_connect()
    query=f"""
WITH base_data AS (
    SELECT 
      --  created_date,
        h3_cell_to_parent(h3_index, {resolution}) as h3_parent_index,
        h3_cell_to_center_child(h3_cell_to_parent(h3_index, {resolution}), 15) as h3_parent_center,
        h3_cell_to_local_ij(
            h3_cell_to_center_child(h3_cell_to_parent(h3_index, {resolution}), 15),
            h3_cell_to_center_child(h3_index, 15)
        ) as ij_coords
    FROM df_calls
),
avg_ij AS (
    SELECT
        COUNT(*)::FLOAT as count,
        h3_parent_index,
        h3_parent_center as h3_parent_center_index,
        AVG(ij_coords[1])::INT as avg_i,
        AVG(ij_coords[2])::INT as avg_j,
      --  created_date
    FROM base_data
    GROUP BY h3_parent_index, h3_parent_center -- created_date
),
normalized AS (
    SELECT 
        *,
        SQRT((count - MIN(count) OVER()) / NULLIF(MAX(count) OVER() - MIN(count) OVER(), 0))::FLOAT as sqrt_ratio,
        h3_local_ij_to_cell(h3_parent_center_index, avg_i::INT, avg_j::INT) AS avg_cell
    FROM avg_ij
)
SELECT 
    h3_h3_to_string(avg_cell) as h3_cell,
    sqrt_ratio,
 --   created_date
FROM normalized
WHERE h3_parent_center_index IS NOT NULL;
"""
    df = con.sql(query).df()
    print(df)
        
    # df = h3_cluster_analysis(resolution)
    # print(df)
    # df = add_rgb(df, 'sqrt_ratio')
    # print(df)
    # df['datetime'] = pd.to_datetime(df['datetime'])
    # print(df['datetime'])
    # # print(df['month'].unique())
    # print(df)
    return df
    
        