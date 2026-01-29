@fused.udf 
def udf(bbox: fused.types.TileGDF = None, release: str = "2024-09-18-0", theme: str = None, overture_type: str = None, use_columns: list = None, num_parts: int = None, min_zoom: int = None, polygon: str = None, point_convert: str = None,
       resolution: int=10,
       min_count: int=0,
        limit: int=10_000):
    from utils import get_overture, get_con
    import shapely 
    import geopandas as gpd
    gdf_overture = get_overture(bbox=bbox, release=release, theme=theme, overture_type=overture_type, use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    # return gdf
    # @fused.cache
    def get_cabs():
        cab_udf = fused.load("stephen.kent.data@gmail.com/DuckDB_H3_Example_for_nyc_builidngs")
        df = fused.run(udf=cab_udf, resolution=resolution, min_count=min_count)
        return df

    cab_df = get_cabs()
    con = get_con()
    query = f"""
    SELECT h3_cell_to_boundary_wkt(cell_id) as boundary, cnt FROM cab_df
    ORDER BY cnt DESC
    LIMIT {limit}
    """
    df = con.sql(query).df()
    gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    # gdf = gdf.to_crs('EPSG:4326')
    gdf_joined = gdf_overture.sjoin(gdf)
    print(gdf_joined)
    return gdf_joined