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
    resolution: int= 12
):
    from utils import get_overture
    import geopandas as gpd
    from shapely import wkt
    import pandas as pd
    @fused.cache
    def get_highway():
        gdf = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2022/PRISECROADS/tl_2022_36_prisecroads.zip')
        gdf = gdf.to_crs("EPSG:4326")
        gdf = gdf[gdf.FULLNAME.str.contains('787', na=False)].copy()
        return gdf
    gdf_787 = get_highway()
    gdf_overture = get_overture(
        bbox=bbox,
        release=release,
        theme=theme,
        overture_type=overture_type,
        use_columns=use_columns,
        num_parts=num_parts,
        min_zoom=min_zoom,
        polygon=polygon,
        point_convert=point_convert
    )
    # Filter using string contains - equivalent to SQL LIKE '%787%'
    # gdf = gdf[gdf.names.str['primary'].str.contains('787', na=False)].copy()
    gdf_joined = gdf_overture.sjoin(gdf_787)
    gdf_joined['metric'] = 100
    duckdb_connect = fused.load(
            "https://github.com/fusedio/udfs/tree/a1c01c6/public/common/"
        ).utils.duckdb_connect
    con = duckdb_connect()
    # Convert geometry to WKT format and create new DataFrame
    
    
    from shapely import wkt

    import geopandas as gpd
    from shapely import wkt
    
    # Convert geometry to WKT using Shapely
    gdf_joined['geometry'] = gdf_joined['geometry'].apply(lambda x: wkt.dumps(x))
    
    # Drop the GeoDataFrame's geometry designation, converting it to a regular pandas DataFrame
    df_787 = pd.DataFrame(gdf_joined)
    
    # Ensure the 'geometry' column is treated as text
    df_787['geometry'] = df_787['geometry'].astype(str)
    
    # Check the column type
    print(df_787['geometry'].dtype)  # Should print 'object', which means it's plain text
    

    query = f"""
    with geometry_cte as (
select
ST_GeomFromText(geometry) AS geom,
CAST(UNNEST(generate_series(1, ST_NPoints(ST_GeomFromText(geometry)))) AS INTEGER) AS point_index
FROM df_787
), to_points as (
 SELECT
 ST_PointN(geom, point_index) AS point
 FROM geometry_cte
 )
 SELECT

h3_h3_to_string(h3_latlng_to_cell(ST_Y(point), ST_X(point), {resolution})) AS cell_id,
count(1) as cnt
from to_points
group by cell_id"""
    df = con.sql(query).df()
    return df

   