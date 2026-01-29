@fused.udf
def udf(start_date: str = '2025-01-01', 
        end_date: str = '2025-01-07', 
        res: int = 4, 
        var: str = 't2m',
        lat: float = 40.7128,  # NYC coordinates as example
        lng: float = -74.0060):
    import pandas as pd
    import h3 
    import xarray 
    import numpy as np
    from datetime import datetime, timedelta
    import plotly.graph_objects as go
    
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    
    # Generate date range
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    
    # Collect temperature data for each date
    all_data = []
    
    for date in dates:
        datestr = date.strftime('%Y-%m-%d')
        
        # Input GCS bucket
        path_in = f'https://storage.googleapis.com/gcp-public-data-arco-era5/raw/date-variable-single_level/{datestr.replace("-","/")}/2m_temperature/surface.nc'
        
        try:
            # Load data
            path = fused.download(path_in, path_in) 
            xds = xarray.open_dataset(path)
            
            # Convert to DataFrame and unstack
            df = xds[var].to_dataframe().unstack(0)
            df.columns = df.columns.droplevel(0)
            
            # Set the H3 res for each observation coordinate
            df['hex'] = df.index.map(lambda x: h3.api.basic_int.latlng_to_cell(x[0], x[1], res))
            df = df.set_index('hex').sort_index()
            
            # Calculate daily mean temperature
            df['daily_mean'] = df.iloc[:, :24].values.mean(axis=1)
            
            # Find the hexagon closest to the specified lat/lng
            target_hex = h3.api.basic_int.latlng_to_cell(lat, lng, res)
            if target_hex in df.index:
                temp_kelvin = df.loc[target_hex, 'daily_mean']
                temp_celsius = temp_kelvin - 273.15  # Convert from Kelvin to Celsius
                
                all_data.append({
                    'date': datestr,
                    'temperature': temp_celsius,
                    'temperature_k': temp_kelvin
                })
                
        except Exception as e:
            print(f"Error processing {datestr}: {e}")
            continue
    
    # Create DataFrame
    df_ts = pd.DataFrame(all_data)
    df_ts['date'] = pd.to_datetime(df_ts['date'])
    
    if len(df_ts) == 0:
        return pd.DataFrame({'date': [], 'temperature': []})
    
    # Create interactive time series plot
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_ts['date'],
        y=df_ts['temperature'],
        mode='lines+markers',
        name='Temperature',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        title=f'Temperature Time Series ({lat}, {lng})',
        xaxis_title='Date',
        yaxis_title='Temperature (°C)',
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    # Add range slider
    fig.update_xaxes(rangeslider_visible=True)
    
    # Convert to HTML
    html_content = f"""
    <div style="font-family: Arial, sans-serif; padding: 20px;">
        <h2>ERA5 Temperature Time Series</h2>
        <p>Location: {lat}, {lng} | Resolution: H3-{res}</p>
        <div style="width: 100%; height: 400px;">
            {fig.to_html(include_plotlyjs='cdn', full_html=False)}
        </div>
        <div style="margin-top: 20px;">
            <h3>Data Summary</h3>
            <p>Period: {start_date} to {end_date}</p>
            <p>Mean Temperature: {df_ts['temperature'].mean():.2f}°C</p>
            <p>Min Temperature: {df_ts['temperature'].min():.2f}°C</p>
            <p>Max Temperature: {df_ts['temperature'].max():.2f}°C</p>
        </div>
    </div>
    """
    
    return common.html_to_obj(html_content)