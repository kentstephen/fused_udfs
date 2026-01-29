@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolutionn: int=10):
    import geopandas as gpd
    import shapely
    import pandas as pd
    import duckdb
    import numpy as np
    from utils import get_walk_data, add_rgb
    bounds = bbox.bounds.values[0]
    
    walk_df = get_walk_data()
    # print(walk_df)
    def get_cells(bounds, walk_df):
        
        
        con = fused.utils.common.duckdb_connect()
        xmin, ymin, xmax, ymax = bounds

        buffer = bbox.z[0] # deespeek change this
        # Here we make cells ake cells and see top power plant fuel type for each cell
        query = f"""    

    SELECT 
        geometry, 
        walk_score
    FROM walk_df
    WHERE ST_XMin(ST_GeomFromText(geometry)) >= ($xmin - 0.009)
      AND ST_XMax(ST_GeomFromText(geometry)) <= ($xmax + 0.009)
      AND ST_YMin(ST_GeomFromText(geometry)) >= ($ymin - 0.009)
      AND ST_YMax(ST_GeomFromText(geometry)) <= ($ymax + 0.009)

        """
        return con.sql(query, params={'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax}).df()
    df = get_cells(bounds, walk_df)
    df = df.drop_duplicates()
    gdf = gpd.GeoDataFrame(df,geometry=df.geometry.apply(shapely.wkt.loads))
    # print(gdf)
    # gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, use_columns=['height'], min_zoom=10)
    # gdf_joined = gdf_overture.sjoin(gdf, how="inner", predicate="intersects")
    @fused.cache(path="s3://fused-users/stephenkentdata/fused-cache/stephen.kent.data@gmail.com/walkability_index/")
    def total_score(walk_df):
        return duckdb.sql('select walk_score from walk_df').df()
    
    @fused.cache(path="s3://fused-users/stephenkentdata/fused-cache/stephen.kent.data@gmail.com/walkability_index/")
    def get_global_boundaries(walk_df):
        # Get all scores
        df_total_score = total_score(walk_df)
        
        # Instead of qcut, let's use fixed ranges based on min/max
        min_score = df_total_score['walk_score'].min()
        max_score = df_total_score['walk_score'].max()
        
        # Create evenly spaced boundaries
        boundaries = np.linspace(min_score, max_score, 8)  # 8 boundaries for 7 bins
        return boundaries
    
    # Usage:
    boundaries = get_global_boundaries(walk_df)
    gdf = add_rgb(gdf, 'walk_score', global_quantiles=boundaries)
    # gdf = add_rgb(gdf, 'walk_score')
    return gdf