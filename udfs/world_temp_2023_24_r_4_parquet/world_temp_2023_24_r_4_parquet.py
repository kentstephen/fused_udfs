@fused.udf
def udf(datestr: str="2024-02-01"):
    path = "s3://fused-users/stephenkentdata/temperature-t2m/world_temp_2023-24_r_4.parquet"
    import duckdb
    import pandas as pd
    import time
    from datetime import datetime
    import h3
    @fused.cache
    def read_parquet(path):
        df = duckdb.sql(f"from read_parquet('{path}')").df()
        df['hex'] = df['hex'].apply(h3.int_to_str)
        return df
    # Read the data
    df = read_parquet(path)
    @fused.cache
    def filter_date(df, datestr):
    # If a date string is provided, filter the data
        if datestr:
            # Convert datestr to datetime for comparison
            filter_date = datetime.strptime(datestr, '%Y-%m-%d')
            
            # Assuming there's a date/datetime column in your dataframe
            # Replace 'date_column' with your actual date column name
            df = df[df['date'] == filter_date]
            return df
    df= filter_date(df, datestr)
    return df