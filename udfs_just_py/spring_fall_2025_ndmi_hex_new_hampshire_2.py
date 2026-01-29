
@fused.udf(cache_max_age="0s")
def udf(
    bounds: fused.types.Bounds,
    provider="MSPC",
    time_of_interest="2025-03-15/2025-06-15",
    res:int = 8
):  
    # common = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/")
    import duckdb
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    def run_fused(args):
        bounds, res, time_of_interest = args
        print(f"Loading data for: {time_of_interest}")  # Add this debug line
        return fused.run("fsh_6X7cOvBOcDCmeEVv7szrx5", bounds=bounds, res=res, time_of_interest=time_of_interest) #token from Sentinel_Tile_Example_moisture_mspc_2_ndwi
    
    arg_list = [
        (bounds, res, "2025-04-15/2025-06-10"),  # Wet period (before drought)
        (bounds, res, "2025-07-01/2025-08-31")   # Dry period (record dry summer)
    ]
    df_spring, df_fall = common.run_pool(run_fused, arg_list, max_workers=2)
    if df_spring is None or df_fall is None or df_spring.empty or df_fall.empty:
       return
    print(df_spring)
    def run_query(df_spring, df_fall):
        import duckdb
        qr ="""
SELECT 
    spring.hex as hex,
    AVG(spring.metric) as ndwi_spring,
    AVG(fall.metric) as ndwi_fall,
    (AVG(fall.metric) - AVG(spring.metric)) as ndwi_change,
    ABS(AVG(fall.metric) - AVG(spring.metric)) as change_magnitude
FROM df_spring spring
INNER JOIN df_fall fall 
    ON spring.hex = fall.hex
GROUP BY spring.hex
        """
        return duckdb.sql(qr).df()
    df = run_query(df_spring,df_fall)
    print(df)
    print(f"ndwi_change describe:{df['ndwi_change'].describe()}")
    return df