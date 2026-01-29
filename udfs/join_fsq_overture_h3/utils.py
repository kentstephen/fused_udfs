import shapely
import pandas as pd
import geopandas as gpd
def get_fsq(bounds):
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    gdf = fused.run("UDF_Foursquare_Open_Source_Places", bounds=bounds, min_zoom=0)

    gdf['lat'] = gdf.geometry.centroid.y
    gdf['lng'] = gdf.geometry.centroid.x

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
b.id,
f.name,
f.geometry as boundary,
f.level1_category_name,
f.level2_category_name,
f.level3_category_name,
f.fsq_category_ids

from df_buildings b inner join df_fsq f
on b.hex = h3_latlng_to_cell(f.lat, f.lng, {resolution})
    """
    # Run the query and return a GeoDataFrame
    df = con.sql(query).df()
    return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))