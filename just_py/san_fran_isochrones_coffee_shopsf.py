@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int=12,
        # costing="pedestrian", 
        # time_steps=[5] # Five minutes
       ):
    import shapely
    import pandas as pd
    from utils import get_iso_gdf, get_cells
    
    # This pulls 5 minute walking isochrones around FSQ coffee shops
    gdf_iso = get_iso_gdf(costing="pedestrian", time_steps=[5])
    print(gdf_iso)
    # print(gdf_iso)
    # Converts geometries to WKT for DuckDB 
    gdf_iso['geometry'] = gdf_iso['geometry'].apply(shapely.wkt.dumps)

    # Better to use Pandas with DuckDB
    df_iso = pd.DataFrame(gdf_iso)
    if df_iso is None or df_iso.empty:
        return  
    
    # Convert the isochrone Polygons to H3
    gdf_h3 = get_cells(df_iso, resolution)

    # Get Overture Buildings
    # gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=10)
    
    # Join H3 with Buildings, using coffe_cnt to visualize
    # gdf_joined = gdf_overture.sjoin(gdf_h3, how="left", predicate="intersects")

    # See the cells 
    return gdf_h3
    
    # return gdf_joined