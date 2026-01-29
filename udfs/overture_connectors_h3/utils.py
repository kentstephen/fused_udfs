import geopandas as gpd
import pandas as pd
import shapely
def get_over(bounds, overture_type):
        overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
        gdf = overture_utils.get_overture(bbox=bounds, overture_type=overture_type, min_zoom=0)
        if gdf is None or gdf.empty:
            return
        if gdf.crs != 'EPSGS:4326':
            gdf.to_crs('EPSG:4326')
        if overture_type=="connector":
            gdf['lat'] = gdf.geometry.centroid.y
            gdf['lng'] = gdf.geometry.centroid.x
            gdf = gdf.drop(columns=['geometry'])
        elif overture_type=='building':
            gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)

def run_query(df_connectors, resolution, bounds):

    xmin, ymin, xmax, ymax = bounds
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""
 SELECT
    h3_latlng_to_cell(lat, lng, {resolution}) as hex,
    h3_cell_to_lat(hex) as cell_lat,
    h3_cell_to_lng(hex) as cell_lng
from df_connectors
where
    h3_cell_to_lat(hex) >= {ymin}
    AND h3_cell_to_lat(hex) < {ymax}
    AND h3_cell_to_lng(hex) >= {xmin}
    AND h3_cell_to_lng(hex) < {xmax}
group by all
    """
    return con.sql(query).df()