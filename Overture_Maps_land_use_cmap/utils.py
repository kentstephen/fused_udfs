def get_overture(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-10-23-0",
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
def add_rgb_cmap(gdf, key_field, cmap_dict):
    import pandas as pd
    def get_rgb(value):
        if pd.isna(value):
            print(f"Warning: NaN value found in {key_field}")
            return [128, 128, 128]  # Default color for NaN values
        if value not in cmap_dict:
            print(f"Warning: No color found for {value}")
        return cmap_dict.get(value, [128, 128, 128])  # Default to gray if not in cmap

    rgb_series = gdf[key_field].apply(get_rgb)
    
    gdf['r'] = rgb_series.apply(lambda x: x[0])
    gdf['g'] = rgb_series.apply(lambda x: x[1])
    gdf['b'] = rgb_series.apply(lambda x: x[2])
    
    
    
    return gdf
CMAP = {
    "agriculture": [255, 223, 186],  # Light orange
    "aquaculture": [173, 216, 230],  # Light blue
    "campground": [205, 133, 63],  # Brown
    "cemetery": [192, 192, 192],  # Silver
    "construction": [255, 165, 0],  # Orange
    "developed": [169, 169, 169],  # Dark gray
    "education": [144, 238, 144],  # Light green
    "entertainment": [255, 10, 80],  # Hot pink
    "golf": [34, 139, 34],  # Forest green
    "grass": [124, 252, 0],  # Lawn green
    "horticulture": [255, 222, 173],  # Light goldenrod
    "landfill": [139, 69, 19],  # Saddle brown
    "managed": [220, 220, 220],  # Gainsboro
    "medical": [255, 99, 71],  # Tomato red
    "military": [0, 128, 128],  # Teal
    "park": [60, 179, 113],  # Medium sea green
    "pedestrian": [250, 128, 114],  # Salmon
    "protected": [85, 107, 47],  # Dark olive green
    "recreation": [100, 149, 237],  # Cornflower blue
    "religious": [153, 50, 204],  # Dark orchid
    "residential": [255, 255, 0],  # Yellow
    "resource_extraction": [139, 0, 0],  # Dark red
    "transportation": [70, 130, 180],  # Steel blue
    "winter_sports": [176, 224, 230],  # Powder blue
}

