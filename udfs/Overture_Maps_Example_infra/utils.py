def get_overture(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-08-20-0",
    theme: str = None,
    overture_type: str = None,
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None):
    import logging
    import concurrent.futures
    import pandas as pd
    import geopandas as gpd
    import json
    from shapely.geometry import shape, box

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


def add_rgb_to_gdf(gdf, cmap, attr):
    import pandas as pd
    """
    Add RGB color values to a GeoDataFrame based on a specified attribute and color map.

    Parameters:
    gdf (GeoDataFrame): The GeoDataFrame to which RGB colors will be added.
    cmap (dict): A dictionary mapping attribute values to RGB color lists.
    attr (str): The attribute in the GeoDataFrame to base the colors on.

    Returns:
    GeoDataFrame: The updated GeoDataFrame with new 'r', 'g', and 'b' columns.
    """
    def get_rgb(row):
        color = cmap.get(row[attr], [0, 0, 0])  # Default to black if attribute is not found
        return pd.Series({'r': color[0], 'g': color[1], 'b': color[2]})

    rgb_columns = gdf.apply(get_rgb, axis=1)
    gdf = pd.concat([gdf, rgb_columns], axis=1)
    
    return gdf

CMAP = {
    "agricultural": [255, 0, 0],      # Bold red
    "civic": [0, 0, 255],             # Bold blue
    "commercial": [0, 255, 0],        # Bold green
    "education": [255, 255, 0],       # Bold yellow
    "entertainment": [255, 0, 255],   # Bold magenta
    "industrial": [0, 255, 255],      # Bold cyan
    "medical": [255, 165, 0],         # Bold orange
    "military": [128, 0, 128],        # Bold purple
    "outbuilding": [255, 20, 147],    # Bold pink
    "religious": [0, 128, 128],       # Bold teal
    "residential": [128, 128, 0],     # Bold olive
    "service": [139, 69, 19],         # Bold saddle brown
    "transportation": [255, 69, 0],   # Bold red-orange
    "aerialway": [75, 0, 130],        # Bold indigo
    "airport": [255, 140, 0],         # Bold dark orange
    "barrier": [218, 165, 32],        # Bold goldenrod
    "bridge": [0, 100, 0],            # Bold dark green
    "communication": [138, 43, 226],  # Bold blue violet
    "manhole": [220, 20, 60],         # Bold crimson
    "pedestrian": [0, 191, 255],      # Bold deep sky blue
    "pier": [255, 105, 180],          # Bold hot pink
    "power": [70, 130, 180],          # Bold steel blue
    "recreation": [255, 215, 0],      # Bold gold
    "tower": [128, 0, 0],             # Bold maroon
    "transit": [199, 21, 133],        # Bold medium violet red
    "utility": [30, 144, 255],        # Bold dodger blue
    "waste_management": [233, 150, 122], # Bold dark salmon
    "water": [0, 0, 139]              # Bold dark blue
}
