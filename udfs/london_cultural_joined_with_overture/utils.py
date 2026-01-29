@fused.cache
def add_rgb_cmap(gdf, key_field, cmap_dict):
    import pandas as pd
    """
    Apply a colormap dictionary to a GeoDataFrame based on a specified key field.

    This function adds 'r', 'g', and 'b' columns to a GeoDataFrame, where the values
    are determined by a colormap dictionary based on the values in a specified key field.

    Args:
    gdf (GeoDataFrame): The GeoDataFrame to which the colormap will be applied.
    key_field (str): The column in the GeoDataFrame whose values will be used to look up the colormap.
    cmap_dict (dict): A dictionary mapping key_field values to RGB color lists.

    Returns:
    GeoDataFrame: The input GeoDataFrame with additional 'r', 'g', and 'b' columns.
    """
    
    gdf[["r", "g", "b"]] = gdf[key_field].apply(
        lambda key_field: pd.Series(cmap_dict.get(key_field, [255, 0, 255]))
    )
    return gdf
CMAP = {
    "centres": [255, 99, 71],
    "Theatre": [255, 0, 0],
    "Archives": [255, 140, 0],
    "venues": [255, 165, 0],
    "studios": [139, 0, 0],
    "manufacturing": [0, 255, 255],
    "galleries": [250, 128, 114],
    "nightclubs": [127, 255, 0],
    "building": [255, 182, 193],
    "Libraries": [75, 0, 130],
    "Makerspaces": [160, 82, 45],
    "making": [255, 182, 193],
    "design": [192, 192, 192],
    "all": [181, 36, 247],
    "workspaces": [205, 92, 92],
    "Cinemas": [220, 20, 60],
    "space": [255, 165, 0],
    "grassroot": [199, 21, 133]
}

@fused.cache
def get_data():
    import geopandas as gpd
    import fiona
    import pandas as pd
    
    # Define the path to your GeoPackage file
    file_path = "/vsicurl/https://data.london.gov.uk/download/cultural-infrastructure-map-2023/b4fe7632-deef-4c10-adb0-bb7eeba82611/cultural_venues_in_GIS_format.gpkg"
    
    # Get the list of layer names
    layer_names = fiona.listlayers(file_path)
    
    # Read each layer and combine them
    gdfs = []
    for layer_name in layer_names:
        gdf = gpd.read_file(file_path, layer=layer_name)
        # Extract the last word from the layer name
        gdf['layer'] = layer_name.split()[-1]
        gdfs.append(gdf)
    
    # Combine all GeoDataFrames
    combined_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
    
    print(f"Combined GeoDataFrame has {len(combined_gdf)} rows and {len(combined_gdf.columns)} columns")
    # print(combined_gdf['layer'].value_counts())
    df = pd.DataFrame({
    'latitude': combined_gdf['latitude'],
    'longitude': combined_gdf['longitude'],
    'type': combined_gdf['layer']
    })
    print(df)
    return df
def get_con():
    import duckdb
    con = duckdb.connect()
    con.sql(""" INSTALL h3 FROM community;
                LOAD h3;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;""")
    return con
def get_overture(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-08-20-0",
    theme: str = None,
    overture_type: str = None,
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None
):
    """Returns Overture data as a GeoDataFrame."""
    import logging
    import concurrent.futures
    import json
    
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import shape, box

    # Load Fused helper functions
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f8f0c0f/public/common/"
    ).utils

    if release == "2024-02-15-alpha-0":
        if overture_type == "administrative_boundary":
            overture_type = "administrativeBoundary"
        elif overture_type == "land_use":
            overture_type = "landUse"
        theme_per_type = {
            "building": "buildings",
            "administrativeBoundary": "admins",
            "place": "places",
            "landUse": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }
    elif release == "2024-03-12-alpha-0":
        theme_per_type = {
            "building": "buildings",
            "administrative_boundary": "admins",
            "place": "places",
            "land_use": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }
    else:
        theme_per_type = {
            "address": "addresses",
            "building": "buildings",
            "infrastructure": "base",
            "land": "base",
            "land_use": "base",
            "water": "base",
            "place": "places",
            "division": "divisions",
            "division_area": "divisions",
            "division_boundary": "divisions",
            "segment": "transportation",
            "connector": "transportation",
        }

    if theme is None:
        theme = theme_per_type.get(overture_type, "buildings")

    if overture_type is None:
        type_per_theme = {v: k for k, v in theme_per_type.items()}
        overture_type = type_per_theme[theme]

    if num_parts is None:
        num_parts = 1 if overture_type != "building" else 5

    if min_zoom is None:
        if theme == "admins" or theme == "divisions":
            min_zoom = 7
        elif theme == "base":
            min_zoom = 9
        else:
            min_zoom = 12

    table_path = f"s3://us-west-2.opendata.source.coop/fused/overture/{release}/theme={theme}/type={overture_type}"
    table_path = table_path.rstrip("/")

    if polygon is not None:
        polygon=gpd.from_features(json.loads(polygon))
        bounds = polygon.geometry.bounds
        bbox = gpd.GeoDataFrame(
            {
                "geometry": [
                    box(
                        bounds.minx.loc[0],
                        bounds.miny.loc[0],
                        bounds.maxx.loc[0],
                        bounds.maxy.loc[0],
                    )
                ]
            }
        )

    def get_part(part):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            return utils.table_to_tile(
                bbox, table=part_path, use_columns=use_columns, min_zoom=min_zoom
            )
        except ValueError:
            return None

    if num_parts > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parts) as pool:
            dfs = list(pool.map(get_part, range(num_parts)))
    else:
        # Don't bother creating a thread pool to do one thing
        dfs = [get_part(0)]

    dfs = [df for df in dfs if df is not None]

    if len(dfs):
        gdf = pd.concat(dfs)

    else:
        logging.warn("Failed to get any data")
        return None

    if point_convert is not None:
        gdf["geometry"] = gdf.geometry.centroid

    return gdf

    