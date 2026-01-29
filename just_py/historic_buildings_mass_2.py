@fused.udf
def udf(
    bbox: fused.types.TileGDF, overture_type="building", clip: bool = False
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
    from util import get_gdf
        # utils = fused.load(
        #     "https://github.com/fusedio/udfs/tree/95872cd/public/common"
        # ).utils
    gdf = get_gdf()
    print(gdf)


    gdf_overture = fused.run(
        "UDF_Overture_Maps_Example",
        theme=theme_type[overture_type],
        overture_type=overture_type,
        bbox=bbox,
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
        gdf_joined["geometry"] = gdf_joined.geometry.centroid
    print(gdf_joined)
    return gdf_joined
