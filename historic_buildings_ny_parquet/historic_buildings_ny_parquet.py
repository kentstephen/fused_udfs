@fused.udf
def udf(
    bbox: fused.types.TileGDF, path: str, overture_type="building", clip: bool = False, resolution: int=8
):
    from utils import get_con
    import pandas as pd
    from shapely import wkt
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
        utils = fused.load(
            "https://github.com/fusedio/udfs/tree/95872cd/public/common"
        ).utils
        gdf = utils.read_gdf_file(path).to_crs("EPSG:4326")
    except Exception as e:
        print("This file seems to not contain geometry.", str(e))
        return

    gdf_overture = fused.run(
        "UDF_Overture_Maps_Example",
        theme=theme_type[overture_type],
        overture_type=overture_type,
        bbox=bbox,
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
    # print(type(gdf_joined))
    # return gdf_joined
    from shapely.wkt import loads

  
    buildings_df = pd.DataFrame(gdf_joined)
    buildings_df['geometry'] = buildings_df['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    buildings_df['geometry'] = buildings_df['geometry'].astype('object') 

    con = get_con()
    buildings_query = """    
    
        WITH latlang AS (
            SELECT       
                ST_Y(ST_Centroid(ST_GeomFromText(geometry))) AS latitude,
                ST_X(ST_Centroid(ST_GeomFromText(geometry))) AS longitude,
            FROM buildings_df
            
        ), to_cells as(
            SELECT
                h3_h3_to_string(h3_latlng_to_cell(latitude, longitude, $resolution)) AS cell_id,
                COUNT(1) AS cnt
            FROM latlang
            GROUP BY 1
            ) select
                cell_id,
                SUM(cnt) as cnt
                from to_cells
                group by cell_id
        
        
            """ 
    df = con.sql(buildings_query, params={'resolution': resolution}).df()
    print(df)
    return df