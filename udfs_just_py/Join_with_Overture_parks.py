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
        gdf['value'] = gdf['geometry'].area / 10
        print(gdf["value"].head(30))
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
    return gdf_joined
