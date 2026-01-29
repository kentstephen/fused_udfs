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
    resolution: int=10,
    limit: int = None,
    place_filter: str = 'medical',
    

):
    where_clauses = {
    'night_life': """WHERE categories.primary LIKE '%restaurant%'
                     OR categories.primary LIKE '%bar%'
                     OR categories.primary LIKE '%dance_club%'
                     OR categories.primary LIKE '%lounge%'""",
    'medical': """WHERE categories.primary = 'hospital'
                 OR categories.primary LIKE '%medical%'"""
}

    # Function to retrieve the corresponding WHERE clause
    def get_where_clause(place_filter):
        return where_clauses.get(place_filter, '')
    
    # Example usage
   
    where_clause = get_where_clause(place_filter)
    
    from utils import get_overture, get_con
    import shapely
    import pandas as pd
    import geopandas as gpd
        
    gdf_1 = get_overture(bbox=bbox, release=release, theme="places", overture_type="place", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    print(bbox)
    gdf_1['geometry'] = gdf_1['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    # print(gdf.columns)
    buildings_df = pd.DataFrame(gdf_1)
    con = get_con()
    query = f"""    
    
       
  
        SELECT
            h3_h3_to_string(h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution})) AS cell_id,
            h3_cell_to_boundary_wkt(cell_id) boundary,
            count(1) as cnt,
         FROM buildings_df
         {where_clause}
         GROUP BY cell_id
       
    """

      
    df = con.sql(query).df()
    gdf_places = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    gdf_buildings = get_overture(bbox=bbox, release=release, theme="buildings", overture_type="building", use_columns=use_columns, num_parts=num_parts, min_zoom=min_zoom, polygon=polygon, point_convert=point_convert)
    gdf_joined = gdf_buildings.sjoin(gdf_places)
    gdf_joined = gdf_joined.set_crs(epsg=4326)
    gdf_joined = gdf_joined.drop(columns=['index_right'])
    print(gdf_joined['cnt'].describe())
    
    return gdf_joined
