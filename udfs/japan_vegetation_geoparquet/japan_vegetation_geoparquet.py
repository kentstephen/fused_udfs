@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolutionn: int=10):
    import geopandas as gpd
    import shapely
    from utils import add_rgb_cmap, cmap

    bounds = bbox.bounds.values[0]
    print(f'bounds = bbox.bounds.values[0]: {bounds}')

        # """
    def get_mesh_bounds(bounds):
        xmin, ymin, xmax, ymax = bounds
        # Add larger buffer and round down/up
        min_mesh = f"{int((ymin-0.3) * 1.5)}{int((xmin-0.3) - 100)}"
        max_mesh = f"{int((ymax+0.3) * 1.5)}{int((xmax+0.3) - 100)}"
        return min_mesh, max_mesh

    def read_data(bounds):
        xmin, ymin, xmax, ymax = bounds
        min_mesh, max_mesh = get_mesh_bounds(bounds)
        con = fused.utils.common.duckdb_connect()
        query="""
SET threads = 15;

with mesh_filter as (
FROM read_parquet('s3://us-west-2.opendata.source.coop/pacificspatial/vegetation-jp/parquet/jp_vegetation25000_en.parquet')
WHERE mesh2_c BETWEEN $min_mesh AND $max_mesh
)        
        
SELECT ST_AsText(GEOM) AS boundary, shoku_en as species 
    FROM mesh_filter
    
  WHERE ST_XMin(GEOM) >= $xmin 
  AND ST_XMax(GEOM) <= $xmax
  AND ST_YMin(GEOM) >= $ymin 
  AND ST_YMax(GEOM) <= $ymax;
        """
        # print(query)
        # url= 
        # return con.sql(query).df()
        # return con.sql(query, params={'min_mesh': min_mesh, 'max_mesh': max_mesh})
        return con.sql(query, params={'min_mesh': min_mesh, 'max_mesh': max_mesh,'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax}).df()
    df = read_data(
        # url = 's3://us-west-2.opendata.source.coop/pacificspatial/vegetation-jp/parquet/jp_vegetation25000_en.parquet',
        bounds=bounds
                  ) 
    if 'species' in df.columns and df['species'] is not None:
        df['species'] = df['species'].str.strip()

    

    # print(df['ST_GeomFromWKB(GEOM)'])
    gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    gdf = add_rgb_cmap(gdf, 'species', cmap)
    print(gdf)
    return gdf


    