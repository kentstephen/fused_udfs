@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    release: str = "2024-11-19",
    min_zoom: int = 0,
    use_columns: list = ["geometry", "name", "fsq_category_ids"],
):
    from utils import join_fsq_categories
    import geopandas as gpd
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/19b5240/public/common/"
    ).utils

    path = f"s3://us-west-2.opendata.source.coop/fused/fsq-os-places/{release}/places/"
    df = utils.table_to_tile(
        bbox, table=path, min_zoom=min_zoom, use_columns=use_columns
    )

    df = join_fsq_categories(df, release=release)
    df = df[df["level1_category_name"].str.contains("Landmarks and Outdoors", case=False, na=False)]
    df = df[df["level3_category_name"].str.contains("Park", case=False, na=False)]
    # df = df[df["level3_category_name"].str.strip().str.casefold() == "Park"]

    gdf_tract = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/36_NEW_YORK/36047/tl_2020_36047_tract20.zip')
    gdf_masked = gpd.overlay(df, gdf_tract, how='intersection')
    return gdf_masked
