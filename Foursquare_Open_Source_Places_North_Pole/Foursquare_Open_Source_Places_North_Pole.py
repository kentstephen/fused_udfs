@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    release: str = "2024-12-03",
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

    if "name" in df.columns:
        df = df[df["name"].str.contains("North Pole", case=False, na=False)]
    else:
        # Return an empty GeoDataFrame
        return gpd.GeoDataFrame({'geometry': []}, crs="EPSG:4326")

    return df
