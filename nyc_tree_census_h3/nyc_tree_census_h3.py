@fused.udf
def udf(bbox: fused.types.TileGDF=None, path: str = "s3://fused-users/stephenkentdata/nyc_tree_census.parquet"):
    resolution = max(min(int(6 + (bbox.z[0] - 10) / 2), 12), 6)


    if resolution > 11:resolution=11
    
    import duckdb
    from utils import get_con, add_rgb_cmap, CMAP
    import geopandas as gpd
    import shapely
    con = get_con()
    query = f"""    
    WITH geometry_cte AS (
        SELECT
            name as subtype,
            latitude,
            longitude
        FROM read_parquet('{path}')
        WHERE name is not null
    ),
    h3_cells_cte AS (
        SELECT
            subtype,
            h3_latlng_to_cell(latitude, longitude, {resolution}) AS cell_id
        FROM geometry_cte
    ),
    subtype_counts AS (
        SELECT
            h3_h3_to_string(cell_id) AS cell_id,
            subtype,
            COUNT(*) AS subtype_count
        FROM h3_cells_cte
        GROUP BY cell_id, subtype
    ),
    most_frequent_subtypes AS (
        SELECT
            cell_id,
            subtype,
            subtype_cnt as cnt
        FROM (
            SELECT
                cell_id,
                subtype,
                subtype_count,
                ROW_NUMBER() OVER (PARTITION BY cell_id ORDER BY subtype_count DESC) AS row_num
            FROM subtype_counts
        ) ranked_subtypes
        WHERE row_num = 1
    )
SELECT 
    cell_id,
   --h3_cell_to_boundary_wkt(cell_id) as boundary,  -- If you need the geometry, you could also add ST_GeomFromText() or just use shapely
    subtype as name,
    cnt
FROM most_frequent_subtypes;

    """


    # con.sql("install 'httpfs'; load 'httpfs'; install spatial; load spatial;")
    df = con.sql(query).df()
    # gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    df = add_rgb_cmap(gdf=gdf, key_field="name", cmap_dict=CMAP)
    print(df)
    return df
