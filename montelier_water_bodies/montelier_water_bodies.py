def udf(bbox=(-72.818472, 44.072055, -72.299101, 44.41482)):
    import pandas as pd
    from shapely import wkt
    import geopandas as gpd
    import pynhd
    from shapely import box

    # Note: fused is not imported, so this decorator might cause an error
    # @fused.cache
    def get_water():
        buffer_distance = 10
        # Fetch flowlines and water bodies
        wd_flowlines = pynhd.WaterData("nhdflowline_network")
        flw_gdf = wd_flowlines.bybox(bbox)
        
        wd_waterbodies = pynhd.WaterData("nhdwaterbody")
        water_bodies_gdf = wd_waterbodies.bybox(bbox)
        
        # Combine flowlines and water bodies into one GeoDataFrame
        combined_gdf = pd.concat([flw_gdf, water_bodies_gdf], ignore_index=True)
        
        # Convert WKT strings to Shapely geometries if needed and set the CRS
        combined_gdf['geometry'] = combined_gdf['geometry'].apply(lambda geom: wkt.loads(geom) if isinstance(geom, str) else geom)
        combined_gdf = gpd.GeoDataFrame(combined_gdf, geometry='geometry')
        
        # Check and set CRS if not already set
        # Check and set CRS if not already set
        if combined_gdf.crs is None:
            combined_gdf.set_crs(epsg=4326, inplace=True)
        
        # Convert to EPSG:3857 for buffering
        combined_gdf = combined_gdf.to_crs(epsg=3857)
        
        # Apply the buffer
        combined_gdf['geometry'] = combined_gdf['geometry'].buffer(buffer_distance)
        
        # Convert back to EPSG:4326
        combined_gdf = combined_gdf.to_crs(4326)
        return combined_gdf

    gdf = get_water()
    print(gdf)
    return gdf