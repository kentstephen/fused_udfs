@fused.udf(cache_max_age="0s")
def udf(
    bounds: fused.types.Bounds,
    provider="MSPC",
    time_of_interest="2025-03-15/2025-06-15",
    res:int = 7
):  
    import duckdb
    df_spring = fused.run('fsh_3kBSq5cKUdVnS0c4J8StlW', bounds=bounds, res=res, time_of_interest="2025-03-15/2025-06-15")
    df_fall = fused.run('fsh_3kBSq5cKUdVnS0c4J8StlW', bounds=bounds, res=res, time_of_interest="2025-06-30/2025-09-30")
    if (df_spring is None or df_spring.empty or 
    df_fall is None or df_fall.empty):
        return
    df = duckdb.sql("""
-- Join spring and fall, calculate change
SELECT 
    spring.hex as hex,
    spring.metric as ndmi_spring,
    fall.metric as ndmi_fall,
    (fall.metric - spring.metric) as ndmi_change,
    abs(fall.metric - spring.metric) as change_magnitude,
 
FROM df_spring spring
INNER JOIN df_fall fall 
    ON spring.hex = fall.hex
    """).df()
    print(df)
    print(f"ndmi_change describe:{df['ndmi_change'].describe()}")
    return df