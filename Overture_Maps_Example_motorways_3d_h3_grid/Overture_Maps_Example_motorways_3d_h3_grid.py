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
    resolution: int = 10,
    grid_distance: int = 3
):
    from utils import get_overture
    import pandas as pd
    import duckdb
    import shapely
    
    
    gdf = get_overture(
        bbox=bbox,
        release=release,
        theme="transportation",
        overture_type="segment",
        use_columns=use_columns,
        num_parts=num_parts,
        min_zoom=min_zoom,
        polygon=polygon,
        point_convert=point_convert
    )
    
    import geopandas as gpd
    from shapely import wkt
    
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

    query = f"""
    with geometry_cte as (
select
ST_GeomFromText(geometry) AS geom,
CAST(UNNEST(generate_series(1, ST_NPoints(ST_GeomFromText(geometry)))) AS INTEGER) AS point_index
FROM df_segment
where class = 'motorway'
), to_points as (
 SELECT
 ST_PointN(geom, point_index) AS point
 FROM geometry_cte
 ), to_cells as(
 SELECT
 h3_latlng_to_cell(ST_Y(point), ST_X(point), {resolution}) AS cell_id,

from to_points
group by cell_id)
SELECT
    h3_h3_to_string(unnest(h3_grid_disk(cell_id, {grid_distance}))) as cell_id
    from to_cells"""
    df_seg_hex = con.sql(query).df()
    
    print(df_seg_hex)
    # area_udf = fused.load("stephen.kent.data@gmail.com/Overture_Maps_Example_h3_area")
    df_buildings = fused.run("fsh_n9X8mjt010iI9KnMVPx84", bbox=bbox, resolution=resolution, min_zoom=0)
    
    query_2 = """
   SELECT
    dsh.cell_id,
                   h3_cell_to_boundary_wkt(dsh.cell_id) boundary,
    SUM(db.tba) /495  as tba
FROM 
    df_seg_hex dsh
INNER JOIN 
    df_buildings db
    ON dsh.cell_id = db.cell_id
GROUP BY 
    dsh.cell_id;


    """
    df = con.sql(query_2).df()
    print(df["tba"].describe())
    # print(df.columns)
    print(df)
    return df
    # gdf_final = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    # return gdf_final
    