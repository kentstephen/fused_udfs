# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, buffer_multiple: float = 1,
       res:int = 9):
    import geopandas as gpd
    import shapely
    import pandas as pd
    # overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=bounds, overture_type="building", min_zoom=0)
    
    if gdf is None or gdf.empty:
        return
    print(gdf.columns)
    # gdf = gdf[(gdf['subtype']=='locality') | (gdf['subtype']=='county')]
    # return gdf
    # if overture_type=="place":
    #     gdf['lat'] = gdf.geometry.centroid.y
    #     gdf['lng'] = gdf.geometry.centroid.x
    #     gdf = gdf.drop(columns=['geometry'])
    # elif overture_type=='building':
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    # return pd.DataFrame(gdf)
    df_overture = pd.DataFrame(gdf)
    @fused.cache
    def get_dem(res):
        return fused.run("fsh_7SXAfMlrkS9twhxZiO2cup", bounds=bounds, res=res)
    df_dem = get_dem(res)
    def run_query(df_overture, df_dem):
        utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
        ).utils
        con = utils.duckdb_connect()
        query="""
        select
        o.geometry as geom_wkt,
       -- o.names.primary,
        
     avg(d.metric) as metric
        from df_overture o inner join df_dem d
        on st_intersects(st_geomfromtext(o.geometry), st_geomfromtext(h3_cell_to_boundary_wkt(d.hex)))
        group by all
        """
        df = con.sql(query).df()
        return gpd.GeoDataFrame(df.drop(columns=['geom_wkt']), geometry=df.geom_wkt.apply(shapely.wkt.loads))

    gdf = run_query(df_overture, df_dem)
    return gdf