import shapely
import geopandas as gpd
import pandas as pd
def prep_for_duck(gdf):
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    return pd.DataFrame(gdf)

def cell_convert(df_overture, df_fsq, resolution):
    con = fused.utils.common.duckdb_connect()
    query=f"""
   with overture_to_cells as (
    select
    h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)),ST_X(ST_GeomFromText(geometry)), {resolution}) as hex,
    AVG(height) as height
    from df_overture
    group by hex
), fsq_categories as (
    select
    h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)),ST_X(ST_GeomFromText(geometry)), {resolution}) as hex,
    level2_category_name,
    count(*) as category_count,
    row_number() over (partition by hex order by count(*) desc) as category_rank
    from df_fsq
    group by hex, level2_category_name
)
select
    o.hex,
    o.height,
    f.level2_category_name as poi_categories,
    f.category_count as cnt
from overture_to_cells o 
inner join fsq_categories f on o.hex = f.hex
where f.category_rank = 1;  -- Only select the top category per hex
    """
    return con.sql(query).df()
def normalize_opacity(cnt_series, weight=0.5):
# Handle zeros and negative values before log transformation
    min_valid = cnt_series[cnt_series > 0].min() if any(cnt_series > 0) else 1
    # Replace zeros and negative values with the minimum valid value
    cnt_series_clean = cnt_series.clip(lower=min_valid)
    
    # Apply log transformation with weight
    log_val = np.log(cnt_series_clean * weight)
    
    # Calculate dynamic log min and log max
    log_min = np.log(cnt_series_clean.min() * weight)
    log_max = np.log(cnt_series_clean.max() * weight)
    
    # Normalize to the range 0-1
    normalized = (log_val - log_min) / (log_max - log_min)
    
    # Handle any remaining NaN or infinite values
    normalized = normalized.fillna(0)
    normalized = np.clip(normalized, 0, 1)
    
    # Scale to the range 25-255 and convert to integers
    return (25 + (normalized * 230)).astype(int)
    