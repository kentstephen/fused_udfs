@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=15):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb

    gdf = gpd.read_file('s3://fused-users/stephenkentdata/bedford_routes_subway.gejson')

    gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(1).to_crs('EPSG:4326')
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    df_routes = pd.DataFrame(gdf)
    con = fused.utils.common.duckdb_connect()
    # Convert the isochrones into H3, count the overlap and keep the POI name
    query = f"""
    with to_cells as (
     select
      unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS hex
     from df_routes
    )
    select 
     hex,
     h3_cell_to_boundary_wkt(hex) as boundary,
     count(*) as route_overlap,
     --string_agg(DISTINCT name, ', ') as poi_names
    from to_cells
    group by hex
    """
    # Run the query and return a GeoDataFrame
    df = con.sql(query).df()
    print(df)
    df = add_rgb(df, 'route_overlap')
    return df
    
    # return gdf
