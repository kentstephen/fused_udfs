

   
import geopandas as gpd

@fused.udf
def udf(bbox: fused.types.TileGDF = None, resolution: int = 6, polygon: gpd.GeoDataFrame = None):
    import duckdb
    from shapely.geometry import box

   
    bounds = bbox.total_bounds
    xmin, ymin, xmax, ymax = bounds
    

    # print(f"Bounds: xmin={xmin}, ymin={ymin}, xmax={xmax}, ymax={ymax}")

    utils = fused.load("https://github.com/fusedio/udfs/tree/f928ee1/public/common/").utils
    h3_utils = fused.load("https://github.com/fusedio/udfs/tree/fb65aff/public/DuckDB_H3_Example/").utils
    con = duckdb.connect(config = {'allow_unsigned_extensions': True})
    h3_utils.load_h3_duckdb(con)
    con.sql(f"""INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;""")
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
        
    
        df = con.sql(query, params={'url': url, 'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax, 'resolution': resolution}).df()
        print(df)
        return df
       


    df = read_data(
        url = 's3://us-west-2.opendata.source.coop/fused/overture/2024-03-12-alpha-0/theme=buildings/type=building/part=*/*.parquet',
        resolution=resolution, 
        bounds=bounds
    )
    
    # gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    print(df)
    return df