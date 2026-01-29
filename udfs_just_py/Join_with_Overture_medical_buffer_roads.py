@fused.udf
def udf(
   bbox: fused.types.TileGDF=None, overture_type="segment", clip: bool = False, 
    buffer_distance: int= 1000,
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
        hos_udf = fused.load("<your-email>/Overture_Maps_Example_hospital")
        gdf = fused.run(
        hos_udf,
        theme="base",
        overture_type="land_use",
        bbox=bbox,
        engine='local'
        )
        gdf = gdf.to_crs(epsg=3857)
        # Apply the buffer
        gdf['geometry'] = gdf['geometry'].buffer(buffer_distance)
        # Convert back to EPSG:4326
        gdf = gdf.to_crs("EPSG:4326")
        print(gdf)
    except Exception as e:
        print("This file seems to not contain geometry.", str(e))
        return
    overture_udf = fused.load('https://github.com/fusedio/udfs/tree/c8c3c40/public/Overture_Maps_Example/')
    gdf_overture = fused.run(
        overture_udf,
        theme=theme_type[overture_type],
        overture_type=overture_type,
        bbox=bbox,
        engine='local',
        min_zoom=0,
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
    gdf_joined = gdf_joined.to_crs(epsg=3857)  # Convert to a projected CRS for accurate distance calculation
    gdf = gdf.to_crs(epsg=3857)
    
    # Calculate the centroid of each hospital
    gdf['centroid'] = gdf.geometry.centroid
    
    # Function to calculate distance to nearest hospital centroid
    def dist_to_nearest_hospital(geom):
        return min(geom.distance(point) for point in gdf['centroid'])
    
    # Calculate the distance from each road segment to the nearest hospital centroid
    gdf_joined['distance'] = gdf_joined.geometry.apply(dist_to_nearest_hospital)
    
    # Normalize the distances
    min_dist = gdf_joined['distance'].min()
    max_dist = gdf_joined['distance'].max()
    gdf_joined['normalized_distance'] = (gdf_joined['distance'] - min_dist) / (max_dist - min_dist) *10
    
    # Convert back to EPSG:4326
    gdf_joined = gdf_joined.to_crs(epsg=4326)
    print(gdf_joined["normalized_distance"])
    return gdf_joined
