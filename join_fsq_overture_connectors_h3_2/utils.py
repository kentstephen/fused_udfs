import shapely
import pandas as pd
import geopandas as gpd
def get_fsq(bounds):
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    gdf = fused.run("UDF_Foursquare_Open_Source_Places", bounds=bounds, min_zoom=0)
    gdf = gdf[gdf['level1_category_name']=='Dining and Drinking']
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
    with fsq_to_cells as (
    select
    h3_latlng_to_cell(lat, lng, {resolution}) as hex,
    count(1) as cnt
    from df_fsq
    group by 1
    ), fsq_to_disk as (
    select
    unnest(h3_grid_disk(hex, 15)) as disk_hex,
    cnt
    from fsq_to_cells 
    )
select
b.hex,
avg(f.cnt) as cnt

from df_buildings b inner join fsq_to_disk f
on b.hex = f.disk_hex
group by 1
    """
    # Run the query and return a GeoDataFrame
    return con.sql(query).df()
    # return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))