import pandas as pd
import shapely 
import geopandas as gpd
def get_over(tile, overture_type):
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    gdf = fused.utils.Overture_Maps_Example.get_overture(tile, release='2024-08-20-0', overture_type=overture_type, use_columns=['geometry','class', 'subtype'], min_zoom=0)
    if gdf is None or gdf.empty:
        return pd.DataFrame({})
    
    if overture_type == "segment":
        gdf = gdf[gdf['subtype']!='water']
        exclude_classes = ['track', 'driveway', 'path', 'footway', 'sidewalk', 'pedestrian', 
                  'cycleway', 'steps', 'crosswalk', 'service', 'waterway', 'bridleway', 'alley']

# Filter the GeoDataFrame to exclude rows with these classes
        gdf = gdf[~gdf['class'].isin(exclude_classes)]
        
        # Filter out rows with null geometries BEFORE estimating UTM
        # gdf = gdf[~gdf.geometry.isna()]
        
        # Now you can safely estimate UTM and buffer
        # if gdf is not None and not gdf.empty:
        #     gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(15).to_crs('EPSG:4326')
        # else:
        #     return pd.DataFrame({})  # Return empty DataFrame instead of None
            
        # Convert to WKT
        # gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        # return pd.DataFrame(gdf)
        return gdf
    elif overture_type == 'connector':
        # gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        gdf['lat'] = gdf.geometry.y 
        gdf['lng'] = gdf.geometry.x
        
        df = pd.DataFrame(gdf)
        return df.drop('geometry', axis=1)
        
    elif overture_type == 'land_use':
        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)
    return pd.DataFrame(gdf)  


def run_query(df_connector, res, bounds):

    xmin, ymin, xmax, ymax = bounds
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query = f"""
     select
 h3_latlng_to_cell(lat, lng, {res}) as hex,
 h3_cell_to_boundary_wkt(hex) as boundary,
 count(1) as cnt

from df_connector
group by 1
    """
#     query=f"""with segment_to_cells as (
#     select 
#         unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'overlap')) as hex,
#         geometry

#    from df_segment
# ),
#  connector_to_cells as (
# select
# h3_latlng_to_cell(lat, lng, {res}) as hex,
# count(1) as cnt

#  from df_connector
#  group by 1
# ) 

# SELECT
# geometry as geom_wkt,
# sum(cnt) as cnt
# FROM 
#   segment_to_cells s 

#  LEFT JOIN 
#  connector_to_cells c ON s.hex = c.hex

# where
#     h3_cell_to_lat(s.hex) >= {ymin}
#     AND h3_cell_to_lat(s.hex) < {ymax}
#     AND h3_cell_to_lng(s.hex) >= {xmin}
#     AND h3_cell_to_lng(s.hex) < {xmax}
# group by 1
#     """
    df = con.sql(query).df()
    return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))