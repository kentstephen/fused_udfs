@fused.udf
def udf(
    bbox: fused.types.TileGDF, overture_type="segment", clip: bool = False,
    buffer_distance: int= None
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
        hospital_udf = fused.load("stephen.kent.data@gmail.com/Overture_Maps_Example_hospital")
        gdf = fused.run(
        hospital_udf,
        theme="base",
        overture_type="land_use",
        bbox=bbox,
        engine='local',
        min_zoom=0
    )
        gdf = gdf.to_crs(epsg=3857)
        gdf['geometry'] = gdf['geometry'].buffer(buffer_distance) 
            # Convert back to EPSG:4326
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
        engine='local',
        min_zoom=0
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
    return gdf_joined
