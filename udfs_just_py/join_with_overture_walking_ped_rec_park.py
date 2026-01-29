@fused.udf
def udf(
    bbox: fused.types.TileGDF, overture_type="building", clip: bool = False,
    resolution: int = 11, buffer_distance: int = 500
):
    from util import get_con, add_rgb
    from shapely import wkt
    import pandas as pd
    import geopandas as gpd
    import numpy as np

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
        s_udf = fused.load("stephen.kent.data@gmail.com/Overture_Maps_Example_rec_park_ped")
        gdf = fused.run(
            s_udf,
            theme="base",
            overture_type="land_use",
            bbox=bbox,
            engine='local'
        )
    
        gdf = gdf.to_crs(epsg=3857)
        gdf['geometry'] = gdf['geometry'].buffer(buffer_distance)
        # gdf = gdf.set_geometry('buffered_geometry')
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
        print("There is no data in this viewport. Please move around to find your data.")
        return

    if clip:
        gdf_joined = gdf_overture.clip(gdf)
    else:
        gdf_joined = gdf_overture.sjoin(gdf)

    
       # Convert geometry to WKT and ensure it's treated as string
    gdf_joined['geometry'] = gdf_joined['geometry'].apply(lambda geom: str(geom.wkt))
    
    # Convert to regular pandas DataFrame
    buildings_df = pd.DataFrame(gdf_joined)
    
    # Explicitly set the dtype of the geometry column to string
    # buildings_df['geometry'] = buildings_df['geometry'].astype(object)
    
    # # You can verify the dtype
    print(buildings_df['geometry'].dtype)
    con = get_con()
    buildings_query = """    
        SELECT
            h3_h3_to_string(
                h3_latlng_to_cell(
                    ST_Y(ST_Centroid(ST_GeomFromText(geometry))),
                    ST_X(ST_Centroid(ST_GeomFromText(geometry))),
                    $resolution
            )) AS cell_id,
                 count(1) as cnt
        FROM buildings_df
        group by 1
    """ 
    df = con.sql(buildings_query, params={'resolution': resolution}).df()
    df = add_rgb(df, 'cnt')  
    print(df)
    
    return df