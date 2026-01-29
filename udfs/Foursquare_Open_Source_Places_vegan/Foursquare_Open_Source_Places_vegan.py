@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    release: str = "2024-11-19",
    min_zoom: int = 10,
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
    df = df[df['level3_category_name'].str.contains('vegan', case=False, na=False)]

# Convert to a 
    gdf = gpd.GeoDataFrame(
    df,
    geometry="geometry",
    crs="EPSG:4326"  # Original CRS in WGS 84
)
    gdf_mercator = gdf.to_crs(epsg=3857)

    # Apply the buffer in meters
    gdf_mercator['geometry'] = gdf_mercator.geometry.buffer(500)  # 500 meters buffer
    
    # Reproject back to the original CRS (EPSG:4326)
    gdf = gdf_mercator.to_crs(epsg=4326)
    return gdf
        # return df
