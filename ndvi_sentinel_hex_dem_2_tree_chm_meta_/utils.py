visualize = fused.load(
    "https://github.com/fusedio/udfs/tree/2b25cb3/public/common/"
).utils.visualize
def df_to_hex(bounds_values, df, res, latlng_cols=("lat", "lng")):
    xmin, ymin, xmax, ymax = bounds_values
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            array_agg(data+5) as agg_data
            FROM df
                        where
    h3_cell_to_lat(hex) >= {ymin}
    AND h3_cell_to_lat(hex) < {ymax}
    AND h3_cell_to_lng(hex) >= {xmin}
    AND h3_cell_to_lng(hex) < {xmax}

            group by 1
          -- order by 1
        """
    con = utils.duckdb_connect()
    return con.query(qr).df()
    

    # return con.query(qr).df()
def aggregate_df_hex(bounds_values,df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import pandas as pd
    import numpy as np
    
    # Convert to hexagons
    df = df_to_hex(bounds_values, df, res=res, latlng_cols=latlng_cols)
    
    # Define aggregation functions that handle null values
    if stats_type == "sum":
        fn = lambda x: pd.Series(x).fillna(0).sum()
    # elif stats_type == "mean":
    #     fn = lambda x: np.maximum(0, np.array([val for val in x if val is not None], dtype=float)).mean()
    else:
        fn = lambda x: pd.Series(x).fillna(10).mean()
    
    # Apply the aggregation function to create the metric column
    df["metric"] = df.agg_data.map(fn)
    
    # Replace any remaining NaN values with 0
    # df["metric"] = df["metric"].fillna(0)
    
    return df
import pandas as pd
import shapely 
import geopandas as gpd
def get_over(tile, overture_type):
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=tile, overture_type=overture_type, min_zoom=0)
    if gdf is None or gdf.empty:
        return pd.DataFrame({})
    
    if overture_type == "segment":
        walking_types = ['motorway', 'primary','secondary', 'tertiary']
        gdf = gdf[gdf['class'].isin(walking_types)]
        
        # Filter out rows with null geometries BEFORE estimating UTM
        # gdf = gdf[~gdf.geometry.isna()]
        
        # Now you can safely estimate UTM and buffer
        if gdf is not None and not gdf.empty:
            gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(15).to_crs('EPSG:4326')
        else:
            return pd.DataFrame({})  # Return empty DataFrame instead of None
            
        # Convert to WKT
        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)
        
    elif overture_type == 'building':
        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)
    elif overture_type == 'land_use':
        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        gdf = gdf[gdf['subtype']=="park"]
        return pd.DataFrame(gdf)

def run_query(df_roads, df_sentinel, res, bounds):

    xmin, ymin, xmax, ymax = bounds
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""--with buildings_to_cells as (
    --select 
        -- unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'center')) as hex,
      --  id,
    --    height
  --  from df_buildings
--),
with roads_to_cells as (
select
 unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'center')) as hex,
 subtype,
 class
 from df_roads
) 

SELECT
  s.hex,
  r.subtype,
  r.class,
  coalesce(s.metric, 0) as metric
 -- s.metric
FROM 
  df_sentinel s 

inner JOIN 
  roads_to_cells r ON s.hex = r.hex

where
    h3_cell_to_lat(s.hex) >= {ymin}
    AND h3_cell_to_lat(s.hex) < {ymax}
    AND h3_cell_to_lng(s.hex) >= {xmin}
    AND h3_cell_to_lng(s.hex) < {xmax}
--group by all
    """
    return con.sql(query).df()
        