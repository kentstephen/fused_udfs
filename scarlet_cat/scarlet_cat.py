@fused.udf
def udf(bounds: fused.types.Tile = None, h3_res: int = 10):
    """
    Change n value to see more or less rotations
    Or edit any of the code directly to see changes live!
    """
    import geopandas as gpd
    import shapely
    import h3
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/2ea46f3/public/Overture_Maps_Example/").utils
    gdf_water = fused.utils.Overture_Maps_Example.get_overture(bounds=bounds,overture_type='water')
    gdf_water = gdf_water[gdf_water['class'].isin(['river', 'stream', 'lagoon', 'pond', 'drain'])]
    gdf_water['cell_id'] = gdf_water.geometry.apply(lambda x: h3.geo_to_cells(x,h3_res))
