@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=8, date: str = '2024-01-01'):
    import geopandas as gpd
    import shapely
    import duckdb
    import pandas as pd
    from utils import get_emergency_calls, get_precincts, add_rgb
    df_calls = get_emergency_calls()
    # print(df_calls['create_date'])
    # df_test = duckdb.sql("select date_trunc('month', cast(create_date as timestamp))  from df_calls --WHERE date_trunc('day', cast(create_date as timestamp)) = '2024-01-11'")
    # print(df_test)
    df_stations = get_precincts()

    def h3_cluster_analysis(df_calls, df_stations):
        con = fused.utils.common.duckdb_connect()
        query=f"""
WITH 
precinct_cells AS (
    SELECT
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) AS station_hex,
        h3_cell_to_parent(
            h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}),
            {resolution - 2}
        ) as parent_hex
    FROM df_stations
),
calls_cells AS (
    SELECT
        h3_latlng_to_cell(latitude, longitude, {resolution}) as calls_hex,
        h3_cell_to_parent(
            h3_latlng_to_cell(latitude, longitude, {resolution}),
            {resolution - 2}
        ) as parent_hex,
        create_date as timestamp,
        count(*) as call_cnt
    FROM df_calls
    where typ_desc LIKE '%MISCHIEF%'
    GROUP BY calls_hex, parent_hex, timestamp
),
station_loads AS (
    SELECT 
        pc.station_hex,
        cc.calls_hex,
        cc.call_cnt,
        cc.timestamp,
        h3_cell_to_local_ij(pc.station_hex, cc.calls_hex) as ij_coords,
        abs(h3_cell_to_local_ij(pc.station_hex, cc.calls_hex)[1]) + 
        abs(h3_cell_to_local_ij(pc.station_hex, cc.calls_hex)[2]) as ij_distance
    FROM precinct_cells pc
    JOIN calls_cells cc ON pc.parent_hex = cc.parent_hex
    WHERE h3_grid_distance(pc.station_hex, cc.calls_hex) <= 15
),
local_loads AS (
    SELECT
        station_hex,
        timestamp,
        sum(CASE WHEN ij_distance = 0 THEN call_cnt ELSE 0 END) as local_call_cnt
    FROM station_loads
    GROUP BY station_hex, timestamp
)
SELECT 
    h3_h3_to_string(sl.station_hex) as hex,
    sl.timestamp,
    sum(
        CASE 
            WHEN ij_distance = 0 THEN call_cnt
            ELSE call_cnt::DOUBLE / ij_distance
        END
    ) as load_score,
    sum(
        CASE
            WHEN ij_distance > 0 THEN 
                (call_cnt::DOUBLE / ij_distance) * 
                CASE 
                    WHEN ll.local_call_cnt = 0 THEN 1.0
                    ELSE 1.0 / ll.local_call_cnt::DOUBLE 
                END
            ELSE 0
        END
    ) as help_score
FROM station_loads sl
JOIN local_loads ll ON sl.station_hex = ll.station_hex 
    AND sl.timestamp = ll.timestamp
GROUP BY hex, sl.timestamp
ORDER BY sl.timestamp, hex
"""
        df = con.sql(query).df()
        return df
    df = h3_cluster_analysis(df_calls, df_stations)
    print(df['help_score'])
    df = add_rgb(df, 'load_score')
    
    # print(df)
    # df['datetime'] = pd.to_datetime(df['datetime'])
    # print(df['datetime'])
    # print(df['month'].unique())
    return df
    
        