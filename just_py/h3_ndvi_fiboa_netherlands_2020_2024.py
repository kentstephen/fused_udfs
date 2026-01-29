# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, res: int = 11):
    # Using common fused functions as helper
    import pandas as pd
    import duckdb
     # Using your existing run_pool function
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    # Define the function to be executed in parallel
    def process_year(time_period, bounds, res):
        """Run the fused.run function for a specific time period and return the dataframe"""
        return fused.run("fsh_1W8TPglX6f6KuaRxSM0J5l", bounds=bounds, time_of_interest=time_period, res=res)
    
    # Create the list of time periods from 2020 to 2024
    time_periods = []
    for year in range(2020, 2025):  # 2020 through 2024
        # First half of the year (Jan-Jun)
        time_periods.append(f"{year}-01-01/{year}-06-30")
        # Second half of the year (Jul-Dec)
        time_periods.append(f"{year}-07-01/{year}-12-31")
        
    # Define the function that will be passed to run_pool
    def process_with_bounds(time_period):
        return process_year(time_period=time_period, bounds=bounds, res=res)
    
    # Execute the function in parallel using the run_pool function
    results = common.run_pool(process_with_bounds, time_periods, max_workers=16)
    valid_results = [df for df in results if df is not None]
    # Concatenate all the resulting dataframes
    if valid_results:
        result_df = pd.concat(valid_results, ignore_index=True)
    else:
        return
    df = duckdb.sql("""SELECT 
    id,
    hex,
    crop_name,
    AVG(metric) AS metric
FROM 
    result_df
GROUP BY 
    id, hex, crop_name
ORDER BY 
    hex""").df()
    # df['metric'] = df['metric'] -5
    return df
