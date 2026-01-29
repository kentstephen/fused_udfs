@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    release: str = "2024-11-19",
    min_zoom: int = 0,
    resolution:int=11,
    use_columns: list = ["geometry", "name", "fsq_category_ids"],
):
    from utils import join_fsq_categories
    import shapely
    import pandas as pd

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/19b5240/public/common/"
    ).utils

    path = f"s3://us-west-2.opendata.source.coop/fused/fsq-os-places/{release}/places/"
    df = utils.table_to_tile(
        bbox, table=path, min_zoom=min_zoom, use_columns=use_columns
    )
    if len(df) < 1:
        return
    # df = df[df['level3_category_name'].str.contains('vegan', case=False, na=False)]
    df = join_fsq_categories(df, release=release)
    if len(df) < 1:
        return
    df = df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
    # df = df[df["name"].str.contains("Starbucks", case=False, na=False)]
    
    # print(df['level3_category_name'])
    df['geometry'] = df['geometry'].apply(shapely.wkt.dumps)

    if df is None or df.empty:
        return pd.DataFrame()
    def get_cells(df, resolution):
        con = fused.utils.common.duckdb_connect()
        query= f"""
        select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) as hex,
        --h3_cell_to_boundary_wkt(hex) boundary,
        count(1) as cnt
        from df
        
        group by 1
     
        """
        df = con.sql(query).df()
        return df
    df = get_cells(df, resolution)
    print(df)
    return df
