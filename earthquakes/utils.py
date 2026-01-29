import pandas as pd
@fused.cache
def get_df():
    df = pd.read_csv('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv')
    return df