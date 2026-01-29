import pandas as pd
import shapely 
import geopandas as gpd
def get_census(tile):
    gdf = fused.run("fsh_4wDMYuGzl08JIx7rGnwYH6", bounds=tile)
    if gdf is None or gdf.empty:
        return pd.DataFrame({})
    gdf = gdf.to_crs('EPSG:4326')
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    return pd.DataFrame(gdf)
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
        return pd.DataFrame(gdf)
    return pd.DataFrame(gdf)  # Default return for other overture_types
# @fused.cache
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
PRAGMA max_temp_directory_size='10GiB';
with roads_to_cells as (
select
 unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'center')) as hex,
GEOID,
"Total  B01003_001" as population,
geometry

 from df_roads
) 

SELECT
  
  r.GEOID,

  r.geometry as boundary,
 avg(s.metric) as metric,
   avg(r.population) as population
FROM 
  df_sentinel s 

 inner JOIN 
  roads_to_cells r ON s.hex = r.hex


  --  h3_cell_to_lat(r.hex) >= {ymin}
    --AND h3_cell_to_lat(r.hex) < {ymax}
    --AND h3_cell_to_lng(r.hex) >= {xmin}
   -- AND h3_cell_to_lng(r.hex) < {xmax}
group by 1,2
--having population >=1
    """
    df = con.sql(query).df()
    return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))