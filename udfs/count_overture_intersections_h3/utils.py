
def get_con():
    import duckdb
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
                LOAD h3ext;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_region='us-west-2';""")
    return con
def load_overture_gdf(
    bbox, overture_type="building", use_columns=None, point_convert=False, min_zoom=8
):
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import box, shape
    from shapely import wkt
    import pyarrow as pa

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
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    gdf = pd.DataFrame(gdf)
    table = pa.Table.from_pandas(gdf)
    return table
