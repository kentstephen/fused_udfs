@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int = 6
        polygon: gpd.GeoDataFrame = None,):
    import duckdb
    import shapely
    from shapely.geometry import box
    import geopandas as gpd

    tile_bbox_gdf = gpd.GeoDataFrame(geometry=[box(-74.25909, 40.477399, -73.700181, 40.916178)], crs="EPSG:4326")
    default_bbox = tile_bbox_gdf.iloc[0].geometry
    tile_bbox_geom = bbox if bbox is not None else default_bbox
    bounds = bbox.bounds.values[0] if bbox is not None else default_bbox.bounds
    print(bounds)

    utils = fused.load("https://github.com/fusedio/udfs/tree/f928ee1/public/common/").utils
    h3_utils = fused.load("https://github.com/fusedio/udfs/tree/fb65aff/public/DuckDB_H3_Example/").utils
    con = duckdb.connect(config = {'allow_unsigned_extensions': True})
    h3_utils.load_h3_duckdb(con)
    con.sql(f"""INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;""")
    
    @fused.cache
    def read_data(url, resolution, bounds):
        xmin, ymin, xmax, ymax = bounds
        query = """
        WITH latlang as (
            SELECT       
                ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))) AS latitude,
                ST_X(ST_Centroid(ST_GeomFromWKB(geometry))) AS longitude
             FROM read_parquet($url)
              WHERE
            bbox.maxx >= $xmin
            AND bbox.minx <= $xmax
            AND bbox.miny <= $ymax
            AND bbox.maxy >= $ymin
        ), to_cells as (
            SELECT
            h3_latlng_to_cell(latitude, longitude, $resolution) AS cell_id,
            COUNT(1) as cnt
            FROM latlang
            GROUP BY 1
        ) SELECT
            h3_h3_to_string(cell_id) AS cell_id,
            SUM(cnt) as cnt
            FROM to_cells
            GROUP BY 1
        """
        # print(query)
        df = con.sql(query, params={'url': url, 'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax, 'resolution': resolution}).df()
        return df
    
    df = read_data(
        url='s3://us-west-2.opendata.source.coop/fused/overture/2024-03-12-alpha-0/theme=buildings/type=building/part=*/*.parquet', 
        resolution=resolution, 
        bounds=bounds
    )
    print(df)
    return df