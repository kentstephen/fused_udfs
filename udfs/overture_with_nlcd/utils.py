import pandas as pd
import shapely 
import geopandas as gpd
def get_over(bounds, overture_type):
        overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
        gdf = overture_utils.get_overture(bbox=bounds, overture_type=overture_type, min_zoom=0)
        if gdf is None or gdf.empty:
            return
        if overture_type=="place":
            gdf['lat'] = gdf.geometry.centroid.y
            gdf['lng'] = gdf.geometry.centroid.x
            gdf = gdf.drop(columns=['geometry'])
        elif overture_type=='building':
            gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)
def run_query(df_buildings, df_lc, res, bounds):

    xmin, ymin, xmax, ymax = bounds
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""
    
with buildings_to_cells as (
    select 
        unnest(h3_polygon_wkt_to_cells(geometry, {res})) as hex,
        id,
        height
    from df_buildings
) 

select
    b.id, 
    b.hex,
    coalesce(avg(b.height), 1) as height,
    coalesce(avg(b.height), 1) + avg(n.elevation) as elevation,
    n.elevation::float as base_elevation,
    n.land_type::varchar as land_type,
    n.color::varchar as color,
    n.n_pixel,
    n.r,
    n.g,
    n.b,
    n.a

FROM buildings_to_cells b 
--INNER JOIN df_dem d ON b.hex = d.hex
inner join df_lc n ON b.hex::uint64 = n.hex::uint64

where
    h3_cell_to_lat(b.hex) >= {ymin}
    AND h3_cell_to_lat(b.hex) < {ymax}
    AND h3_cell_to_lng(b.hex) >= {xmin}
    AND h3_cell_to_lng(b.hex) < {xmax}
group by all
    """
    return con.sql(query).df()