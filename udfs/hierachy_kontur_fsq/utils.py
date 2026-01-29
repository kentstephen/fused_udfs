import pandas as pd
import geopandas as gpd
import shapely

def get_fsq(bounds):
        # overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
        gdf = fused.run("UDF_Foursquare_Open_Source_Places", bounds=bounds, min_zoom=0)
        gdf = gdf[gdf['level1_category_name']=='Health and Medicine']
        if gdf is None or gdf.empty:
            return pd.DataFrame({})
        gdf['lat'] = gdf.geometry.centroid.y
        gdf['lng'] = gdf.geometry.centroid.x

        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)

def run_query(df_kontur, df_fsq, resolution, res_offset, bounds):
    xmin, ymin, xmax, ymax = bounds
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""
with fsq_to_cells as (
    select
    h3_latlng_to_cell(lat, lng, {resolution}) as hex,
    count(1) as cnt
    from df_fsq 
    group by 1
), to_parent as ( 
    select
        h3_cell_to_parent(k.hex, {resolution - res_offset}) as hex,
        avg(k.pop) as pop,
        avg(coalesce(f.cnt, 0)) as cnt
    from df_kontur k 
    left join fsq_to_cells f 
    on h3_cell_to_parent(k.hex, {resolution - res_offset}) = h3_cell_to_parent(f.hex, {resolution - res_offset})
    group by 1
)
select
    hex,
    pop,
    cnt,
    h3_cell_to_lat(hex) as cell_lat,
    h3_cell_to_lng(hex) as cell_lng
from to_parent 
WHERE cell_lat >= {ymin}
AND cell_lat < {ymax}
AND cell_lng >= {xmin}
AND cell_lng < {xmax}


"""
    return con.sql(query).df()

