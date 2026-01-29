@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int=10,
        costing="pedestrian", 
        time_steps=[5]
       ):
    
    import shapely
    import geopandas as gpd
    import pandas as pd
    from utils import get_iso_gdf, get_cells
    
    # This pulls 5 minute walking isochrones around FSQ coffee shops
    gdf_iso = get_iso_gdf(costing=costing, time_steps=time_steps)
    
    # Converts geometries to WKT for DuckDB 
    gdf_iso['geometry'] = gdf_iso['geometry'].apply(shapely.wkt.dumps)

    # Better to use Pandas with DuckDB
    df_iso = pd.DataFrame(gdf_iso)
    if df_iso is None or df_iso.empty:
        return  
    
    # Convert the isochrone Polygons to H3
    gdf_h3 = get_cells(df_iso, resolution)

    # Get NYC boundaries from NYC Open Data
    gdf_nyc = gpd.read_file('https://data.cityofnewyork.us/api/geospatial/j2bc-fus8?method=export&format=GeoJSON')

    # Filter for Bushwick and dissolve
    gdf_bushwick = gdf_nyc[gdf_nyc['ntaname'].str.contains('Bushwick', na=False)].dissolve(by='ntaname').reset_index()
    
    # Keep only geometry column for overlay
    gdf_bushwick = gdf_bushwick[['geometry']]
    
    # Perform overlay (intersection)
    gdf_h3 = gpd.overlay(gdf_h3, gdf_bushwick, how='intersection')
    
    # Get Overture Buildings
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=10)
    
    # Join H3 with Buildings using coffe_score to visualize, you can change to a left join
    gdf_joined = gdf_overture.sjoin(gdf_h3, how="inner", predicate="intersects")

    # See the cells 
    # return gdf_h3
    
    return gdf_joined