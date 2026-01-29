@fused.udf
def udf(bbox: fused.types.TileGDF = None, resolution: int = 10):
    import geopandas as gpd
    import shapely
    import pandas as pd
    import duckdb
    import numpy as np
    from utils import get_walk_data, add_rgb

    # Get the bounds of the tile
    # bounds = bbox.bounds.values[0]

    

    def table_to_tile(bbox, data):
        """
        Args:
            bbox: GeoDataFrame with x,y,z coordinates and geometry
            data: PyArrow Table with a geometry column
        """
        import geopandas as gpd
        
        # Get zoom level
        try:
            x, y, z = bbox[["x", "y", "z"]].iloc[0]
        except:
            z = 0
            
        # Normalize bbox
        if len(bbox) > 1:
            bbox = bbox.dissolve().reset_index(drop=True)
        else:
            bbox = bbox.reset_index(drop=True)
            
        # Convert PyArrow Table to GeoDataFrame for spatial operations
        gdf = data
        
        # Filter data that intersects with bbox
        filtered_df = gdf[gdf.intersects(bbox.geometry[0])]
        filtered_df.crs = bbox.crs
        
        # Convert back to PyArrow Table
        return filtered_df
    walk_df = get_walk_data()
    # Fetch the data within the bounding box
    gdf = table_to_tile(bbox=bbox, data=walk_df)

    @fused.cache(path="s3://fused-users/stephenkentdata/fused-cache/<your-email>/walkability_index/")
    def total_score(walk_df):
        # Return the 'walk_score' column directly from the GeoDataFrame
        return walk_df["walk_score"]
    
    @fused.cache(path="s3://fused-users/stephenkentdata/fused-cache/<your-email>/walkability_index/")
    def get_global_boundaries(walk_df):
        # Use the walk_score column directly to compute boundaries
        min_score = walk_df["walk_score"].min()
        print(min_score)
        max_score = walk_df["walk_score"].max()
        boundaries = np.linspace(min_score, max_score, 8)  # 8 boundaries for 7 bins
        return boundaries

    # Add RGB values based on walkability scores
    boundaries = get_global_boundaries(walk_df)
    # gdf = add_rgb(gdf, "walk_score", global_quantiles=boundaries)

    return gdf