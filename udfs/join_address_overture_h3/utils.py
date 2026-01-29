import shapely
import pandas as pd
import geopandas as gpd
def get_over(bounds, overture_type):
        overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
        gdf = overture_utils.get_overture(bbox=bounds, overture_type=overture_type, min_zoom=0)
        if gdf is None or gdf.empty:
            return
 
        gdf['lat'] = gdf.geometry.centroid.y
        gdf['lng'] = gdf.geometry.centroid.x
            # gdf = gdf.drop(columns=['geometry'])
      
        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)
def join_h3_buildings_with_fsq(df_buildings, df_fsq, resolution):
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    # Convert the isochrones into H3, count the overlap and keep the POI name
    query = f"""
select
id_overture_building_id: b.id,
f.geometry as boundary,
address_id: f.id,
f.*

from df_buildings b inner join df_fsq f
on b.hex = h3_latlng_to_cell(f.lat, f.lng, {resolution})
    """
    # Run the query and return a GeoDataFrame
    df = con.sql(query).df()
    return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))