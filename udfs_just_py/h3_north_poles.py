@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=5,
        costing = "auto", 
        time_steps = [60]):
    # print(bbox)
    x, y, z = bbox.iloc[0][["x", "y", "z"]]
    # from utils import get_fsq_isochrones_gdf
    import geopandas as gpd
    import shapely
    from utils import get_fsq_isochrones_gdf
    @fused.cache
    def north_pole(bbox):
        return fused.run("fsh_4HKnRIycuMDe4usOR7PB8t", x=x, y=y, z=z)
    df_poles = north_pole(bbox)
    def add_latitude_score(df):
        """
        Adds latitude score based on geometry y coordinate (latitude)
        """
        # Get latitude from geometry
        distance = abs(90 - df.geometry.y)
        df['latitude_score'] = (1 + (distance / 180) * 14).round().astype(int)
        return df
    
    df = add_latitude_score(df=df_poles)
    df_poles['geometry'] = df_poles['geometry'].apply(shapely.wkt.dumps)
    print(df_poles.columns)
    if df_poles is None or df_poles.empty:
        return
    # df_poles['latitude'] = df_poles.geometry.y

    
 

    # Example usage:
    
    @fused.cache
    def get_cells(df_poles, resolution):
        con = fused.utils.common.duckdb_connect()
        query=f"""
       select
        h3_h3_to_string(h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), latitude_score::INT)) AS hex,
       h3_cell_to_boundary_wkt(hex) as boundary,
        h3_cell_to_lat(hex)::FLOAT as lat,
        count(1) as cnt
        from df_poles
        group by 1

        
        """
        df = con.sql(query).df()
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))

    df = get_cells(df_poles, resolution)
    print(df)
    # gdf_fsq_isochrones = get_fsq_isochrones_gdf(df, costing, time_steps)
    return df
    # return gdf_fsq_isochrones
    