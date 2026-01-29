@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int = 8):
    import duckdb
    import shapely
    import geopandas as gpd
    from utils import get_bbox
    
    # Call your get_bbox function to retrieve the bounding box and WKT geometry
    fetched_bbox, wkt_geom = get_bbox()
    
    # Convert the bbox dictionary into a Shapely Polygon
    default_bbox_polygon = shapely.geometry.box(
        fetched_bbox['xmin'], 
        fetched_bbox['ymin'], 
        fetched_bbox['xmax'], 
        fetched_bbox['ymax']
    )
    
    # Create a GeoDataFrame for the bbox
    tile_bbox_gdf = gpd.GeoDataFrame(
        {"geometry": [default_bbox_polygon]}, 
        crs="EPSG:4326"
    )

    default_bbox = tile_bbox_gdf.iloc[0].geometry
    tile_bbox_geom = bbox if bbox is not None else default_bbox

    # Set bounds based on the provided or default bbox
    bounds = bbox.bounds if bbox is not None else default_bbox.bounds
    xmin, ymin, xmax, ymax = bounds
    print(bounds)

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils
    h3_utils = fused.load(
        "https://github.com/fusedio/udfs/tree/fb65aff/public/DuckDB_H3_Example/"
    ).utils
    con = duckdb.connect(config = {'allow_unsigned_extensions': True})

    h3_utils.load_h3_duckdb(con)
    con.sql(f"""INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;""")
    
    @fused.cache
    def read_data(url, resolution):
        # Extract the bounds from the bbox to use in the query
        query = f"""
            WITH geo AS (
                SELECT ST_Centroid(ST_GeomFromWKB(geometry)) AS geometry 
                FROM read_parquet($url)
                WHERE 
                    bbox.xmin >= $xmin
                    AND bbox.xmin <= $xmax
                    AND bbox.ymax >= $ymin
                    AND bbox.ymin <= $ymax
            ),
            to_cells AS (
                SELECT 
                    h3_latlng_to_cell(ST_Y(geometry), ST_X(geometry), $resolution) AS cell_id,
                    COUNT(1) AS cnt
                FROM geo
                GROUP BY cell_id
            )
            SELECT 
                cell_id,
                SUM(cnt) AS cnt
            FROM to_cells
            GROUP BY cell_id
        """
        print(query)

        f = con.sql(query, params={'url': url, 'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax,'resolution': resolution}).df()
        return df

    df = read_data(
        url='s3://overturemaps-us-west-2/release/2024-07-22.0/theme=buildings/type=building/*', 
        resolution=resolution
    )
    
    print(df)
    return df
