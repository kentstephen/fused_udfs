@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=13):
    import geopandas as gpd
    import shapely
    from utils import get_walk_polygons, add_rgb
    bounds = bbox.bounds.values[0]
    
    walk_df = get_walk_polygons()
    print(walk_df)
    def get_cells(bounds, walk_df, resolution):
        con = fused.utils.common.duckdb_connect()
        xmin, ymin, xmax, ymax = bounds
        # Here we make cells ake cells and see top power plant fuel type for each cell
        query = f"""    
 with to_cells as (
    SELECT 
        unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS hex, 
        walk_score
    FROM walk_df
    WHERE ST_XMin(ST_GeomFromText(geometry)) >= ($xmin - 0.001)
      AND ST_XMax(ST_GeomFromText(geometry)) <= ($xmax + 0.01)
      AND ST_YMin(ST_GeomFromText(geometry)) >= ($ymin - 0.01)
      AND ST_YMax(ST_GeomFromText(geometry)) <= ($ymax + 0.01)
) select hex, sum(walk_score) as walk_score from to_cells group by 1
        """
        return con.sql(query, params={'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax}).df()
    df = get_cells(bounds, walk_df, resolution)
    # gdf = gpd.GeoDataFrame(df,geometry=df.geometry.apply(shapely.wkt.loads))
    # # print(gdf)
    # # gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, use_columns=['height'], min_zoom=10)
    # # gdf_joined = gdf_overture.sjoin(gdf, how="inner", predicate="intersects")
    df = add_rgb(df, 'walk_score')
    return df