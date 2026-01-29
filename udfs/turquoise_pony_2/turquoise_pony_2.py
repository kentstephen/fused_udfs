# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None,  
        time_of_interest="2020-01-01/2020-04-30",
        h3_size:int=12,
       scale:float=0.081):
        import pandas as pd
        import geopandas as gpd
        import shapely
        # Using common fused functions as helper
        common = fused.load("https://github.com/fusedio/udfs/tree/cd73dbd/public/common/").utils
        # This helper function turns our bounds into XYZ tiles
        tiles = common.get_tiles(bounds, target_num_tiles=20)
        # Buffering tiles internally
        gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bounds=bounds, min_zoom=0)
        if len(gdf_overture) < 1:
            return
        gdf_overture["lat"] = gdf_overture.geometry.centroid.y
        gdf_overture["lng"] = gdf_overture.geometry.centroid.x
        gdf_overture['geometry'] = gdf_overture['geometry'].apply(shapely.wkt.dumps)
        df_overture = pd.DataFrame(gdf_overture)
        df_sentinel = fused.run("fsh_12nrKTwale78mO1HhJNiw9",bounds=bounds, time_of_interest=time_of_interest, h3_size=h3_size,scale=scale)
        print(df_sentinel)
        def get_add_rgb(df_overture, df_sentinel, h3_size):
            duckdb_connect = fused.load(
                "https://github.com/fusedio/udfs/tree/a1c01c6/public/common/"
            ).utils.duckdb_connect
            con = duckdb_connect()
            query=f"""
            with to_cells as (
            select
            unnest(h3_polygon_wkt_to_cells(geometry, {h3_size})) as hex,
            id,
            geometry as geom_wkt,
            height,
        
            from df_overture
            
            )
                     
            select
            o.id,
            o.geom_wkt,
            o.height as height,
            avg(s.agg_band1) as agg_band1, 
            avg(s.agg_band2) as agg_band2,
            avg(s.agg_band3) as agg_band3
            from to_cells o inner join df_sentinel s
            on o.hex = s.hex
            group by all
            """
            df = con.sql(query).df()
            return gpd.GeoDataFrame(df.drop(columns=['geom_wkt']), geometry=df.geom_wkt.apply(shapely.wkt.loads))
        gdf = get_add_rgb(df_overture, df_sentinel, h3_size)
        return gdf