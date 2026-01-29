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
    con = fused.utils.common.duckdb_connect()
    query=f"""


with avg_ij AS (
    SELECT
    create_date,
        COUNT(*)::INTEGER AS count,
    MAX(COUNT(*)) OVER ()::INTEGER AS max_count,
    SQRT(COUNT(*)::FLOAT / MAX(COUNT(*)) OVER ())::FLOAT AS sqrt_ratio,
   
        h3_cell_to_parent(h3_index, {resolution}) as h3_parent_index,
         h3_cell_to_center_child(h3_parent_index, 15) as h3_parent_center_index,
    AVG(h3_cell_to_local_ij(
         h3_cell_to_center_child(h3_cell_to_parent(h3_index, {resolution}), 15),
         h3_cell_to_center_child(h3_index, 15)
       )[1]::INT) AS avg_i,
    AVG(h3_cell_to_local_ij(
         h3_cell_to_center_child(h3_cell_to_parent(h3_index, {resolution}), 15),
         h3_cell_to_center_child(h3_index, 15)
       )[2]::INT) AS avg_j
       
      
    FROM df_calls
  
    GROUP BY h3_parent_index, create_date
)

SELECT h3_h3_to_string(h3_cell_to_parent(h3_local_ij_to_cell(h3_parent_center_index, avg_i::INT, avg_j::INT), {resolution})) AS avg_cell,
sqrt_ratio,
create_date

FROM avg_ij

"""
    df = con.sql(query).df()
        
    # df = h3_cluster_analysis(resolution)
    # print(df)
    # df = add_rgb(df, 'sqrt_ratio')
    # print(df)
    # df['datetime'] = pd.to_datetime(df['datetime'])
    # print(df['datetime'])
    # print(df['month'].unique())
    print(df)
    return df
    
        