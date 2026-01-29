@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    release: str = "2024-11-19",
    min_zoom: int = 0,
    use_columns: list = ["geometry", "name", "fsq_category_ids"],
):
    from utils import join_fsq_categories

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/19b5240/public/common/"
    ).utils

    path = f"s3://us-west-2.opendata.source.coop/fused/fsq-os-places/{release}/places/"
    df = utils.table_to_tile(
        bbox, table=path, min_zoom=min_zoom, use_columns=use_columns
    )

    # df = df[df['level3_category_name'].str.contains('vegan', case=False, na=False)]
    df = join_fsq_categories(df, release=release)
    df = df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
    df = df[df["name"].str.contains("Starbucks", case=False, na=False)]
    
    print(df['level2_category_name'])
    return df
