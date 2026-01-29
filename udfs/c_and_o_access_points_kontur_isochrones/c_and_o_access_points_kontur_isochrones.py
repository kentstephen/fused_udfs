@fused.udf
def udf(bbox: fused.types.TileGDF=None,
        costing: str= "auto",
        timesteps=[30],
        n: int=10):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb
    def get_single_isochrone(point_data):
        # Function for single isochrone
        point, costing, time_steps = point_data
        try:
            return fused.utils.Get_Isochrone.get_isochrone(
                lat=point.y,
                lng=point.x, 
                costing=costing,
                time_steps=time_steps
            )
        except Exception as e:
            print(f"Error processing point ({point.x}, {point.y}): {str(e)}")
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
            
    @fused.cache(path="s3://fused-users/stephenkentdata/fused-cache/stephen.kent.data@gmail.com/c_and_o/iosochrones")
    def get_pool_isochrones(df, costing, time_steps):
        # Run the isochrone requests concurrently
        if len(df) == 0:
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        # Using the Fused common run_pool function 
        arg_list = [(point, costing, time_steps) for point in df.geometry]
        isochrones = fused.utils.common.run_pool(get_single_isochrone, arg_list, max_workers=100)
        
        # Track which isochrones are valid along with their names
        valid_pairs = [(iso, name) for iso, name in zip(isochrones, df['access_point']) if len(iso) > 0]
        if not valid_pairs:
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        
        # Unzip the pairs and add names to each isochrone in the pair
        valid_isochrones, names = zip(*valid_pairs)
        result = pd.concat(valid_isochrones)
        
        # Add names by repeating each name for its corresponding isochrone's rows
        name_list = []
        for iso, name in zip(valid_isochrones, names):
            name_list.extend([name] * len(iso))
        result['access_point'] = name_list
        
        return result
    def get_fsq_isochrones_gdf(costing, time_steps): 
        # Greater Bushwick
        bbox = gpd.GeoDataFrame(
           geometry=[shapely.box(-73.966036,40.666722,-73.875359,40.726179)], 
           crs=4326
        )
        # Coffee shops
        df = fused.run("fsh_5CcWesP6vhv5l3ljknRsOu")
    
        if len(df) == 0:
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
            
        # Concurrent isochrones  
        return get_pool_isochrones(df, costing, time_steps)
    gdf = get_fsq_isochrones_gdf(costing, timesteps)
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    df_iso = pd.DataFrame(gdf)
    def isochrones_to_h3(df_iso):
    # Connect to DuckDB
        con = fused.utils.common.duckdb_connect()
        # Convert the isochrones into H3, count the overlap and keep the POI name
        query = f"""
        with to_cells as (
         select
          unnest(h3_polygon_wkt_to_cells(geometry, 8)) AS hex,
          access_point
         from df_iso
        )
        select 
         hex,
         --h3_cell_to_boundary_wkt(hex) as boundary,
         count(*) as cnt,
         string_agg(DISTINCT access_point, ', ') as access_points
        from to_cells
        group by hex
        """
        # Run the query and return a GeoDataFrame
        return con.sql(query).df()
    # return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    df_iso = isochrones_to_h3(df_iso)
    # gdf_canal = 
    @fused.cache
    def get_kontur(df_iso):
        bbox = gpd.GeoDataFrame(
           geometry=[shapely.box(-79.056,38.7311,-76.8738,40.0032)], # canal
           crs=4326
        )
        gdf_canal = fused.run("fsh_1NSGUBnnFpXOcjtndfxom2",bbox=bbox)
        gdf_canal = gdf_canal.dissolve()
        gdf_canal['geometry']= gdf_canal['geometry'].apply(shapely.wkt.dumps)
        df_canal = pd.DataFrame(gdf_canal)
        df_kontur = fused.run("fsh_1Kg290kb0BPNahbrby4Kah", bbox=bbox)
        con = fused.utils.common.duckdb_connect()
        query="""
            select 
            i.hex,
            i.cnt,
            i.access_points,
            k.pop
            from df_iso i inner join df_kontur k on i.hex = k.hex
            join df_canal c on st_disjoint(st_geomfromtext(c.geometry), st_geomfromtext(h3_cell_to_boundary_wkt(i.hex)))
        """
        return con.sql(query).df()
    df= get_kontur(df_iso)
    df = add_rgb(df, 'cnt')
    return df

