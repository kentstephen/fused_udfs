@fused.udf
def udf(datestr: str='2025-01-01', res:int=4, var='t2m'):
    import pandas as pd
    import h3 
    import xarray 
    import io
    import pyarrow.parquet as pq
    import pyarrow as pa
    import json

    # Input GCS bucket
    path_in=f'https://storage.googleapis.com/gcp-public-data-arco-era5/raw/date-variable-single_level/{datestr.replace("-","/")}/2m_temperature/surface.nc'

    # Load data
    path = fused.download(path_in, path_in) 
    xds = xarray.open_dataset(path)

    # Convert to DataFrame and unstack
    df = xds[var].to_dataframe().unstack(0)
    df.columns = df.columns.droplevel(0)

    # Set the H3 res 15 for each observation coordinate
    df['hex'] = df.index.map(lambda x:h3.api.basic_int.latlng_to_cell(x[0],x[1],res))
    df = df.set_index('hex').sort_index()

    # Aggregate metrics
    df.columns=[f'hour{hr}' for hr in range(24)]
    df['daily_mean'] = df.iloc[:,:24].values.mean(axis=1)

    # Render
    df['hex']=df.index
    df['metric'] = df['hour1']
    out = df[['hex', 'metric']].reset_index(drop=True)
    print(out.head(3).T)
    return out


    