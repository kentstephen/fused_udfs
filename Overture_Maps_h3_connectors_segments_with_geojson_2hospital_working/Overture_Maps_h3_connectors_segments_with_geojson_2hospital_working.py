@fused.udf 
def udf(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-09-18-0",
    theme: str = None,
    overture_type: str = None, 
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None,
    resolution: int = 11,
    k_ring: int = 10  # Ensure the type hint is here
):
    from utils import get_overture, get_con
    import shapely
    import pandas as pd
    import geopandas as gpd
        
    # Step 1: Fetch hospital data using the first query
    gdf_1 = get_overture(bbox=bbox, release=release, theme="places", overture_type="place", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    
    gdf_1['geometry'] = gdf_1['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    buildings_df = pd.DataFrame(gdf_1)
    con = get_con()

    query_1 = """    
        SELECT
            h3_h3_to_string(h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), $resolution)) AS cell_id,
            count(1) as cnt
        FROM buildings_df
        WHERE categories.primary = 'hospital'
        GROUP BY cell_id;
    """
    
    df_hospital_h3 = con.sql(query_1, params={'resolution': resolution}).df()

    # Step 2: Loop through the DataFrame and dynamically substitute cell_id and k_ring into the second query
    dfs = []
    
    for cell_id in df_hospital_h3['cell_id']:
        query_2 = f"""
            WITH to_grid AS (
                SELECT
                    '{cell_id}' AS cell_id,
                    h3_grid_disk('{cell_id}', {k_ring}) AS disk_cells
            ),
            extract AS (
                SELECT 
                    UNNEST(disk_cells) AS hex,
                    h3_grid_distance('{cell_id}', UNNEST(disk_cells)) AS distance_
                FROM to_grid
            )
            SELECT
                hex AS cell_id,
                h3_cell_to_boundary_wkt(hex) AS boundary,
                (1.0 / distance_) * 100 AS distance
            FROM extract;
        """
        
        # Execute the dynamically created query
        df = con.execute(query_2).df()  # Execute and fetch the DataFrame
        dfs.append(df)

    # Concatenate all DataFrames into one
    final_df = pd.concat(dfs, ignore_index=True)

    # Convert 'boundary' column to geometries and create GeoDataFrame
    gdf_places = gpd.GeoDataFrame(final_df.drop(columns=['boundary']), geometry=final_df['boundary'].apply(shapely.wkt.loads))

    # Additional processing
    gdf_segment = get_overture(bbox=bbox, release=release, theme="transportation", overture_type="segment", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    gdf_segment = gdf_segment[~gdf_segment['class'].isin(['track', 'driveway', 'path', 'footway', 'sidewalk', 'pedestrian', 'cycleway', 'steps', 'crosswalk', 'bridleway', 'alley', 'ferry'])]
    gdf_segment = gdf_segment[gdf_segment['subtype'] != 'water']

    # Spatial join and final result
    gdf_joined = gdf_segment.sjoin(gdf_places, how='inner', predicate='intersects')
    gdf_joined = gdf_joined.set_crs(epsg=4326)
    gdf_joined = gdf_joined.drop(columns=['index_right'])

    print(gdf_joined['distance'].describe())
    print(final_df)
    
    return gdf_joined
