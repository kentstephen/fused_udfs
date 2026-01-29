@fused.udf
def udf(
    bbox: fused.types.TileGDF = None, overture_type="building", clip: bool = False
):
    import geopandas as gpd
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
        from util import get_gdf
        gdf = get_gdf()
        print(f"Mass gdf: {gdf.geometry}")
    except Exception as e:
        print("This file seems to not contain geometry.", str(e))
    
        return
    
    gdf_overture = fused.run(
        "UDF_Overture_Maps_Example",
        theme=theme_type[overture_type],
        overture_type=overture_type,
        bbox=bbox,
        min_zoom=0,
    )
    print(f"Overture gdf: {gdf_overture.geometry}")
    if len(gdf_overture) == 0:
        print(
            "There is no data in this viewport. Please move around to find your data."
        )
        
        return
    if clip:
        gdf_joined = gdf_overture.clip(gdf)
    else:
        gdf_joined = gdf.sjoin(gdf_overture)


        # gdf_joined["geometry"] = gdf_joined.geometry.centroid
    print(f"Mass gdf CRS{gdf.crs}")
    print(f"Overture gdf CRS:{gdf_overture.crs}")

    print(f"gdf_joined: {gdf_joined}")
    return gdf_joined
