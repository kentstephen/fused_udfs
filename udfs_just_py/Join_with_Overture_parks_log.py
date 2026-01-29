@fused.udf
def udf(
    bbox: fused.types.TileGDF = None, overture_type="building", clip: bool = False,
    buffer_distance = 200
):
    theme_type = {
        "building": "buildings",
        "segment": "transportation",
        "connector": "transportation",
        "place": "places",
        "address": "addresses",
        "water": "base",
        "land_use": "base",
        "infrastructure": "base",
        "land": "base",
        "division": "divisions",
        "division_area": "divisions",
        "division_boundary": "divisions",
    }
    try:
        park_udf = fused.load("<your-email>/Overture_Maps_Example_parks")
        gdf = fused.run(
        park_udf,
        theme="base",
        overture_type="land_use",
        bbox=bbox,
    )
        gdf = gdf.to_crs(epsg=3857)
        # Calculate the area of each geometry in square meters
        import numpy as np
        import numpy as np
        from shapely.ops import unary_union
        import geopandas as gpd
        
        # Step 1: Apply the buffer to geometries
        gdf['geometry'] = gdf['geometry'].buffer(buffer_distance)
        
        # Step 2: Perform a union of all geometries to combine overlaps
        # This ensures that overlapping buffers are merged into one geometry
        gdf_union = gdf.unary_union  # Perform a union on all geometries
        
        # Step 3: Recreate the GeoDataFrame from the union of geometries
        gdf = gpd.GeoDataFrame(geometry=[gdf_union], crs=gdf.crs)
        
        # Step 4: Calculate the area of the combined geometry and its logarithm
        gdf['area'] = gdf['geometry'].area
        gdf['log_area'] = np.log(gdf['area']).round(3)
        
        # Print the result to verify
        print(gdf[['log_area']])

        
        
        
        # Now 'sum_log_area' contains the sum of log_area values for each geometry, including overlaps
        # print(gdf[['geometry', 'log_area', 'sum_log_area']].head())

        print(gdf['log_area'].head())
        gdf = gdf.to_crs("EPSG:4326")
    except Exception as e:
        print("This file seems to not contain geometry.", str(e))
        return
    overture_udf = fused.load('https://github.com/fusedio/udfs/tree/c8c3c40/public/Overture_Maps_Example/')
    gdf_overture = fused.run(
        overture_udf,
        theme=theme_type[overture_type],
        overture_type=overture_type,
        bbox=bbox,
        engine='local'
    )
    if len(gdf_overture) == 0:
        print(
            "There is no data in this viewport. Please move around to find your data."
        )
        return
    if clip:
        gdf_joined = gdf_overture.clip(gdf)
    else:
        gdf_joined = gdf_overture.sjoin(gdf)
    print(gdf_joined["log_area"].tail(20))
    return gdf_joined
