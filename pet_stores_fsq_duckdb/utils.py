import pandas as pd
import geopandas as gpd

def get_fsq_points(bbox):
    # Pull the points
    df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    
    # Check if the df is empty
    if len(df) < 1:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        
    pet_ids = ['4bf58dd8d48988d100951735', '56aa371be4b08b9a8d573508', '5032897c91d4c4b30a586d69']
    
    # Filter by category, safely handling None values with a simple or condition
    df = df[df['fsq_category_ids'].apply(lambda x: any(id in x for id in pet_ids) if x is not None else False)]
    
    # Extract coordinates
    df['lat'] = df.geometry.y
    df['lng'] = df.geometry.x
    df = df.drop(columns=['geometry'])
    
    return pd.DataFrame(df)

def run_query(df, resolution, bounds):
    xmin, ymin, xmax, ymax = bounds
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""with to_cells as(
    select 
    hex: h3_latlng_to_cell(lat, lng, {resolution}),
    cnt: count(1)
    from df
    group by 1),
    to_disk as (
    select
    unnest(h3_grid_disk(hex, 2)) as hex,
    cnt
    from to_cells)
    select
    hex,
    h3_cell_to_lat(hex) as cell_lat,
    h3_cell_to_lng(hex) as cell_lng,
    cnt: avg(cnt)
    from to_disk
    where
        cell_lat >= {ymin}
        AND cell_lat < {ymax}
        AND cell_lng >= {xmin}
        AND cell_lng < {xmax}
    group by 1
    """
    return con.sql(query).df()