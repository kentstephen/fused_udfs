# @fused.cache
def table_to_tile(bounds):
    import pandas as pd
    import shapely
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    gdf = fused.run("fsh_6cOfhwzXwxYj3d8rTjJaDD", bounds=bounds)
    print(gdf)
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    return pd.DataFrame(gdf)
# @fused.cache
def run_query(res, bounds):
    import pandas as pd
    import geopandas as gpd
    import shapely
    df_fields = table_to_tile(bounds)
    print(df_fields.columns)
    df_cdl_hex = fused.run("fsh_1YA2Qj5GncNwOycnfa5Idr", bounds=bounds, res=res)
    if df_fields is None or len(df_fields) == 0 or df_fields.shape[0] == 0:
        
        return None
    if df_cdl_hex is None or len(df_cdl_hex) ==0 or df_cdl_hex.empty:
        return None
    # xmin, ymin, xmax, ymax = 
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""
WITH fields_to_cells AS (
    SELECT
        unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'center')) as hex,
        geometry
    FROM df_fields
),
field_crop_summary AS (
    SELECT 
        f.geometry,
        h.crop_type,
        SUM(h.n_pixel) as total_n_pixel,
        FIRST(h.r) as r,
        FIRST(h.g) as g,
        FIRST(h.b) as b,
        FIRST(h.a) as a,
        ROW_NUMBER() OVER(PARTITION BY f.geometry ORDER BY SUM(h.n_pixel) DESC) as crop_rank
    FROM fields_to_cells f
    INNER JOIN df_cdl_hex h ON f.hex = h.hex
    GROUP BY f.geometry, h.crop_type
)
SELECT
    geometry as boundary,
    crop_type,
    total_n_pixel,
    r, g, b, a
FROM field_crop_summary
WHERE crop_rank = 1
--group by all
    """
        # Run the query and return as a gdf
    df = con.sql(query).df()
    return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    
# where
#     h3_cell_to_lat(hex) >= {ymin}
#     AND h3_cell_to_lat(hex) < {ymax}
#     AND h3_cell_to_lng(hex) >= {xmin}
#     AND h3_cell_to_lng(hex) < {xmax}
    