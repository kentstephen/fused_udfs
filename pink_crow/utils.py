import pandas as pd
import geopandas as gpd
import pandas as pd
from shapely import wkb, wkt

def load_overture_gdf(
    bbox, overture_type="building", use_columns=None, point_convert=False, min_zoom=0
):
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import box, shape

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/9fcbd1a/public/common/"
    ).utils

    # 1. Structure S3 path
    theme_per_type = {
        "building": "buildings",
        "administrative_boundary": "admins",
        "place": "places",
        "land_use": "base",
        "water": "base",
        "segment": "transportation",
        "connector": "transportation",
    }

    release = "2024-03-12-alpha-0"
    theme = theme_per_type.get(overture_type, "buildings")
    table_path = f"s3://us-west-2.opendata.source.coop/fused/overture/{release}/theme={theme}/type={overture_type}".rstrip(
        "/"
    )

    # Helper function to load data
    def get_part(
        part, bbox=bbox, num_parts=5, table_path=table_path, min_zoom=min_zoom
    ):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            # Simplify data if zoomed-out to prevent browser overload
            if min_zoom is None:
                if theme in ["admins", "base"]:
                    min_zoom = 7
                else:
                    min_zoom = 12
            # Load data
            return utils.table_to_tile(
                bbox, table=part_path, use_columns=use_columns, min_zoom=min_zoom
            )
        except ValueError:
            return None

    # 2. Load data
    # If `buildings`, load in parallel
    if overture_type == "building":
        gdf = pd.concat(utils.run_pool(get_part, range(5)))
    else:
        gdf = get_part(0, num_parts=1)

    if point_convert:
        gdf["geometry"] = gdf.geometry.centroid
    return gdf


import pandas as pd
from shapely import wkb, wkt

def gdf_to_pandas_with_wkt(gdf):
    # Convert the GeoDataFrame to a regular pandas DataFrame
    df = pd.DataFrame(gdf)
    
    # Convert geometry to WKB and then to Shapely geometry
    df['geometry'] = df['geometry'].apply(lambda geom: wkb.loads(geom.wkb) if geom is not None else None)
    
    # Now convert the geometry to WKT format
    df['geometry'] = df['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    
    # Finally, apply WKT loads to ensure all geometries are in WKT format
    df['geometry'] = df['geometry'].apply(lambda wkt_str: wkt.loads(wkt_str) if wkt_str is not None else None)
    
    return df