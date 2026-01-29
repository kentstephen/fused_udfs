def get_overture(bounds):
        import pandas as pd
        import shapely
        gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=bounds, overture_type="segment", min_zoom=0)
        df_segment = pd.DataFrame(gdf)
        # Convert geometry to WKT using Shapely
        gdf['geometry'] = gdf['geometry'].apply(lambda x: shapely.wkt.dumps(x))
        df = pd.DataFrame(gdf)
        return df
def run_query(resolution, df_segment, con):
        query = f"""
            WITH geometry_cte AS (
                SELECT 
                    ST_GeomFromText(geometry) AS geom,
                    CAST(UNNEST(generate_series(1, ST_NPoints(ST_GeomFromText(geometry)))) AS INTEGER) AS point_index,
                    class
                FROM df_segment
                WHERE
                        
                     class NOT IN ('track', 'driveway', 'path', 'footway', 'sidewalk', 'pedestrian', 'cycleway', 'steps', 'crosswalk', 'bridleway', 'alley')
                        
                        ),
                points_cte AS (
                SELECT
                    class,
                    ST_PointN(geom, point_index) AS point
                FROM geometry_cte
                ),
                h3_cells AS (
                SELECT
                    h3_h3_to_string(h3_latlng_to_cell(ST_Y(point), ST_X(point), {resolution})) AS cell_id,
                    CASE
                    WHEN class = 'motorway' THEN 10
                    WHEN class = 'primary' THEN 8
                    WHEN class = 'secondary' THEN 5
                    WHEN class = 'tertiary' THEN 3
                    WHEN class = 'residential' OR class IS NULL THEN 1
                    ELSE 1
                    END AS value
                FROM points_cte
                ),
                aggregated_cells AS (
                SELECT
                    cell_id,
                    SUM(value) AS total_value
                FROM h3_cells
                GROUP BY cell_id
                )
                SELECT cell_id, total_value FROM aggregated_cells;
                """
        df = con.sql(query).df()
        return df
def add_rgb(df, value_column, n_quantiles=10):
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import pandas as pd
    import numpy as np

    # Check if the DataFrame has data
    if df.empty or df[value_column].nunique() < 2:
        # Add default RGB columns with NaNs or a default color
        df[['r', 'g', 'b']] = np.nan
        return df

    # Calculate quantiles for the value column
    quantiles = pd.qcut(df[value_column], q=n_quantiles, labels=False, duplicates='drop')
    
    # Normalize the quantile values between 0 and 1
    norm = mcolors.Normalize(vmin=quantiles.min(), vmax=quantiles.max())
    cmap = plt.cm.cividis  # Put the color map name here at the end - case sensitive
    
    # Function to convert normalized quantile values to RGB
    def map_to_rgb(q):
        color = cmap(norm(q))
        r = int(color[0] * 255)
        g = int(color[1] * 255)
        b = int(color[2] * 255)
        return r, g, b
    
    # Apply function and add RGB columns to DataFrame using quantile values
    df[['r', 'g', 'b']] = quantiles.apply(map_to_rgb).apply(pd.Series)
    return df
    
    