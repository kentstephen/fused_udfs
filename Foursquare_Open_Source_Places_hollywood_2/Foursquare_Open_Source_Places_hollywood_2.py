@fused.udf
def udf(
    bounds: fused.types.Bounds,
    release: str = "2025-01-10",
    min_zoom: int = 0,
    use_columns: list = ["geometry", "name", "fsq_category_ids"],
):
    import pandas as pd
    from utils import join_fsq_categories
    utils = fused.load("https://github.com/fusedio/udfs/tree/d0e8eb0/public/common/").utils
    path = f"s3://us-west-2.opendata.source.coop/fused/fsq-os-places/{release}/places/"
    
    # Load initial data
    df = utils.table_to_tile(
        bounds, table=path, min_zoom=min_zoom, use_columns=use_columns
    )
    if len(df) < 1:
        return pd.DataFrame({})
    # Filter out null names and non-Hollywood names immediately
    df = df[df['name'].notna()]
    
    @fused.cache
    def filter_hollywood(df):
        return df[df["name"].str.contains("Hollywood", case=False, na=False)]
    
    # Apply the cached filter
    df = filter_hollywood(df)
    
    
    
    # Only join categories after filtering to a much smaller dataset
    df = join_fsq_categories(df, release=release)
    
    return df
