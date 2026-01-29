@fused.udf
def udf(bbox: fused.types.TileGDF = None, 
        res: int = 10
    ):
    import geopandas as gpd
    import shapely
    import pandas as pd
    import pyarrow as pa
    bounds = bbox.bounds.values[0]
    # Get the boundaries for NYC boroughs.
    gdf_boundary = gpd.read_file('https://data.cityofnewyork.us/api/geospatial/tqmj-j8zm?method=export&format=GeoJSON')
    gdf_boundary = gdf_boundary.dissolve().reset_index(drop=True)
    # Transform the GeoDataFrame to EPSG:4326
    gdf_boundary = gdf_boundary.to_crs('EPSG:4326')
    print(gdf_boundary.crs)
    
    
    @fused.cache
    def get_trees():
        df = pd.read_csv(
            "https://data.cityofnewyork.us/api/views/5rq2-4hqu/rows.csv?accessType=DOWNLOAD"
        )
        df["latitude"] = df["Latitude"]
        df["name"] = df["spc_common"]
        table = pa.Table.from_pandas(df)
        return table

    tree_table = get_trees()
    @fused.cache
    def get_tree_cells(tree_table, res, bounds):
        xmin, ymin, xmax, ymax = bounds
        con = fused.utils.common.duckdb_connect()
        query = f"""
        SELECT h3_latlng_to_cell(latitude, longitude, {res}) AS cell_id, 
        h3_cell_to_boundary_wkt(cell_id) boundary,
        count(1) as cnt
        FROM tree_table
        
        GROUP BY 1"""
       
        df = con.sql(query).df()
        return gpd.GeoDataFrame(
            df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads)
        )

    gdf_trees = get_tree_cells(tree_table=tree_table, res=res, bounds=bounds)
    # @fused.cache
    def get_overture(bbox, gdf_boundary):
        gdf= fused.utils.Overture_Maps_Example.get_overture(bbox=bbox)
        # gdf_masked = gdf.clip(gdf_boundary)
        return gdf
        
    gdf_overture = get_overture(bbox=bbox, gdf_boundary=gdf_boundary)
    # Load borough boundaries for NYC
    
    # Perform spatial intersection to mask gdf_overture with the tracts
    # 
   
    
        
    gdf_joined = gdf_overture.sjoin(gdf_trees, how='left', predicate='intersects')
       
    # gdf_joined = mask_overture(gdf_overture=gdf_overture,gdf_boundary=gdf_boundary, gdf_trees=gdf_trees)
    # print(gdf_joined["cnt"].describe())
    # To see the cells, uncomment the following
    # return gdf_trees
    print(gdf_joined["height"])
    return gdf_joined
