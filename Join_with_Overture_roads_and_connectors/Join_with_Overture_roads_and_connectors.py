@fused.udf
def udf(
    bbox: fused.types.TileGDF, overture_type="segment", clip: bool = False
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
        join_udf = fused.load('https://github.com/fusedio/udfs/tree/c8c3c40/public/Overture_Maps_Example/')
        gdf = fused.run(
        join_udf,
        theme="transportation",
        overture_type="connector",
        bbox=bbox,
        engine='local'
        
    )
      
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
    walking_classes = ['track', 'path', 'footway', 'pedestrian', 'steps', 'cycleway', 'crosswalk', 'alley']
    gdf_overture = gdf_overture[~gdf_overture['class'].isin(walking_classes)]
    if len(gdf_overture) == 0:
        print(
            "There is no data in this viewport. Please move around to find your data."
        )
        return
    
    # Add cnt column to gdf (connectors)
    gdf['cnt'] = 1
    
    if clip:
        gdf_joined = gdf_overture.clip(gdf)
    else:
        gdf_joined = gdf_overture.sjoin(gdf)
    
    # Sum the cnt for each road segment
    gdf_joined['cnt'] = gdf_joined.groupby(level=0)['cnt'].sum().fillna(1)
    
    print(gdf_joined['cnt'])
    return gdf_joined