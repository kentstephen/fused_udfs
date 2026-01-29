@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    bounds = bbox.bounds.values[0]
    path = 's3://us-west-2.opendata.source.coop/fiboa/nl-ref/nl_ref.parquet'

    def get_fields(bounds, path):
        xmin, ymin, xmax, ymax = bounds
        print(f"Bounds: xmin={xmin}, ymin={ymin}, xmax={xmax}, ymax={ymax}")
        con = fused.utils.common.duckdb_connect()
        query = f"""
        select st_astext(ST_Transform(geometry, 'EPSG:28992', 'EPSG:4326', always_xy := true)) as geometry, id
        from read_parquet($path)
WHERE ST_XMin(ST_Transform(geometry, 'EPSG:28992', 'EPSG:4326', always_xy := true)) >= ($xmin - 0.5)
  AND ST_XMax(ST_Transform(geometry, 'EPSG:28992', 'EPSG:4326', always_xy := true)) <= ($xmax + 0.5)
  AND ST_YMin(ST_Transform(geometry, 'EPSG:28992', 'EPSG:4326', always_xy := true)) >= ($ymin - 0.5)
  AND ST_YMax(ST_Transform(geometry, 'EPSG:28992', 'EPSG:4326', always_xy := true)) <= ($ymax + 0.5)
        """
        
        # print(query)
        # url= 
        # return con.sql(query).df()
        # return con.sql(query, params={'min_mesh': min_mesh, 'max_mesh': max_mesh})
       
        
        return con.sql(query, params={'path': path,'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax}).df()
    df = get_fields(bounds, path)
    print(df)
    gdf = gpd.GeoDataFrame(df,geometry=df.geometry.apply(shapely.wkt.loads))
    print(gdf)
    return gdf