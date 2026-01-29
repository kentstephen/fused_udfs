def get_overture(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-03-12-alpha-0",
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
    else:
        theme_per_type = {
            "building": "buildings",
            "administrative_boundary": "admins",
            "place": "places",
            "land_use": "base",
            "water": "base",
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
        if theme == "admins":
            min_zoom = 0
        elif theme == "base":
            min_zoom = 0
        else:
            min_zoom = 0

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
import duckdb
def get_con():
    
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
                LOAD h3ext;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_region='us-west-2';""")
    return con
import pandas as pd
def add_rgb_cmap(gdf, key_field, cmap_dict):

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
  "agricultural": [34, 139, 34],  # Green for agriculture
  "civic": [0, 128, 128],         # Teal for civic buildings
  "commercial": [255, 165, 0],    # Orange for commercial buildings
  "education": [70, 130, 180],    # Steel blue for education
  "entertainment": [255, 105, 180], # Hot pink for entertainment
  "industrial": [128, 128, 128],  # Gray for industrial
  "medical": [255, 0, 0],         # Red for medical
  "military": [139, 69, 19],      # Saddle brown for military
  "outbuilding": [169, 169, 169], # Dark gray for outbuildings
  "religious": [148, 0, 211],     # Dark violet for religious buildings
  "residential": [255, 255, 0],   # Yellow for residential
  "service": [0, 0, 255],         # Blue for service buildings
  "transportation": [255, 140, 0] # Dark orange for transportation
}
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

def add_rgb_to_df_dynamic(df, value_column):
    # Dynamically calculate min and max values from the DataFrame
    min_value = df[value_column].min()
    max_value = df[value_column].max()
    
    # Normalize the column values between 0 and 1
    norm = mcolors.Normalize(vmin=min_value, vmax=max_value)
    cmap = plt.cm.plasma  # You can choose other color maps like 'plasma', 'inferno', etc.
    
    # Function to convert normalized values to RGB
    def map_to_rgb(value):
        color = cmap(norm(value))
        # Convert to 0-255 scale and round
        return int(color[0] * 255), int(color[1] * 255), int(color[2] * 255)
    
    # Apply function and add RGB columns to DataFrame
    df[['r', 'g', 'b']] = df[value_column].apply(map_to_rgb).apply(pd.Series)
    return df

# Example usage
df = add_rgb_to_df_dynamic(df, 'tba')  # The 'tba' column is used to dete
