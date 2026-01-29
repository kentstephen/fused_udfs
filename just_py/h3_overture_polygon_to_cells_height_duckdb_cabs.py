@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
       resolution: int= 14, min_count: int=1):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb
    import numpy as np

    gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=0)
    if gdf is None or gdf.empty:
        return pd.DataFrame()

    # Convert geometry to WKT using Shapely
    gdf['geometry'] = gdf['geometry'].apply(lambda x: shapely.wkt.dumps(x))
    df_buildings = pd.DataFrame(gdf)
    @fused.cache
    def get_cabs(resolution, min_count):
        return fused.run("UDF_H3_Hexagon_Layer_Example", resolution=resolution, min_count=min_count)

    df_cabs = get_cabs(resolution, min_count)
    df_cabs = df_cabs[['cell_id', 'cnt']]
    if df_cabs is None or df_cabs.empty:
        return
    def run_query(resolution, df_buildings, df_cabs):
        con = fused.utils.common.duckdb_connect()
        query = f"""
        
      with to_cells as (
    SELECT 
        unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS hex,
        COALESCE(height, 1) as height
    FROM df_buildings
),
aggregated as (
    SELECT 
        b.hex,
        NULLIF(SUM(COALESCE(c.cnt, 0)), 0) as cnt,  -- Prevent zero values
        NULLIF(AVG(b.height), 0) as height
    FROM to_cells b 
    inner JOIN df_cabs c ON b.hex = h3_string_to_h3(c.cell_id)
    GROUP BY hex
)
SELECT 
    hex,
    COALESCE(cnt, 1) as cnt,  -- Replace NULL with 1 to avoid log(0)
    COALESCE(height, 1) as height  -- Replace NULL with 1
FROM aggregated
WHERE hex IS NOT NULL
        """
        return con.sql(query).df()
    # def normalize_opacity(cnt_series):
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
    df = run_query(resolution, df_buildings, df_cabs)
    
    df['opacity'] = normalize_opacity(df['cnt'])
    # print(df["height"].describe())
    # df = add_rgb(df, 'height')
    print(df)
    return df