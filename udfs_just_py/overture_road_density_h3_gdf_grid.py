@fused.udf 
def udf(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-10-23-0",
    theme: str = None,
    overture_type: str = None, 
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = 0,
    polygon: str = None,
    point_convert: str = None,
    resolution: int = 8,
    grid_distance: int= 1
):
    print(bbox.z)
    from utils import get_overture, add_rgb
    import shapely
    # resolution = 6 if bbox.z[0] < 8 else max(min(int(7 + (bbox.z[0] - 8) * (4/6)), 11), 7)
    print(resolution)
    gdf = get_overture(
        bbox=bbox,
        release=release,
        theme=theme,
        overture_type="segment",
        use_columns=use_columns,
        num_parts=num_parts,
        min_zoom=min_zoom,
        polygon=polygon,
        point_convert=point_convert
    )
    # return gdf
    import geopandas as gpd
    from shapely import wkt
    import pandas as pd
    if gdf is None or gdf.empty:
        return pd.DataFrame()
    # Convert geometry to WKT using Shapely
    gdf['geometry'] = gdf['geometry'].apply(lambda x: wkt.dumps(x))
    # print(f"this is gdf_joined {gdf}")
    # Drop the GeoDataFrame's geometry designation, converting it to a regular pandas DataFrame
    df_segment = pd.DataFrame(gdf)
    
    # Ensure the 'geometry' column is treated as text
    df_segment['geometry'] = df_segment['geometry'].astype(str)
    
    # Check the column type
    # print(f"df_segment dtype:{df_segment['geometry'].dtype}")  # Should print 'object', which means it's plain text
    duckdb_connect = fused.load(
            "https://github.com/fusedio/udfs/tree/a1c01c6/public/common/"
        ).utils.duckdb_connect
    con = duckdb_connect()
    def run_query(resolution, df_segment):
        query = f"""
            WITH geometry_cte AS (
                SELECT 
                    ST_GeomFromText(geometry) AS geom,
                    CAST(UNNEST(generate_series(1, ST_NPoints(ST_GeomFromText(geometry)))) AS INTEGER) AS point_index,
                    class
                FROM df_segment
                WHERE
                        
                     class NOT IN ('track', 'driveway', 'path', 'footway', 'sidewalk', 'pedestrian', 'cycleway', 'steps', 'crosswalk', 'bridleway', 'alley')
                        
                        ),
                points_cte AS (
                SELECT
                    class,
                    ST_PointN(geom, point_index) AS point
                FROM geometry_cte
                ),
                h3_cells AS (
                SELECT
                    h3_latlng_to_cell(ST_Y(point), ST_X(point), {resolution}) AS cell_id,
                    CASE
                    WHEN class = 'motorway' THEN 10
                    WHEN class = 'primary' THEN 8
                    WHEN class = 'secondary' THEN 5
                    WHEN class = 'tertiary' THEN 3
                    WHEN class = 'residential' OR class IS NULL THEN 1
                    ELSE 1
                    END AS value
                FROM points_cte
                )
                SELECT 
                h3_h3_to_string(unnest(h3_grid_disk(cell_id, {grid_distance}))) as cell_id,
                h3_cell_to_boundary_wkt(cell_id) as boundary,
                AVG(value) as total_value
                from h3_cells
                group by cell_id
 
                """
        df = con.sql(query).df()
        return df
    df = run_query(resolution, df_segment)
    # gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    df = add_rgb(df, 'total_value')
    # print(df)
    return df
    