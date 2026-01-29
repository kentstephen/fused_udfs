def add_rgb(gdf, cmap, attr):
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

CMAP_1 = {
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
CMAP = {
    "agricultural": [124, 252, 0],       # Fresh green (fields and crops)
    "civic": [70, 130, 180],            # Steel blue (formal and institutional)
    "commercial": [255, 165, 0],        # Vibrant orange (economic activity)
    "education": [255, 255, 0],         # Bright yellow (knowledge and enlightenment)
    "entertainment": [255, 105, 180],   # Hot pink (fun and energy)
    "industrial": [169, 169, 169],      # Dark gray (factories and machinery)
    "medical": [255, 0, 0],             # Bright red (health and urgency)
    "military": [0, 100, 0],            # Dark green (camouflage and discipline)
    "outbuilding": [210, 180, 140],     # Tan (barns and storage)
    "religious": [128, 0, 128],         # Purple (spirituality and tradition)
    "residential": [184, 134, 11],      # Goldenrod (homes and warmth)
    "service": [139, 69, 19],           # Saddle brown (utility and practicality)
    "transportation": [0, 0, 255],      # Bright blue (movement and connectivity)
    "aerialway": [75, 0, 130],          # Indigo (elevated pathways)
    "airport": [135, 206, 235],         # Sky blue (air travel)
    "barrier": [169, 169, 169],         # Dark gray (protection and obstruction)
    "bridge": [255, 215, 0],            # Gold (sturdy and visible)
    "communication": [138, 43, 226],    # Blue violet (technology and signals)
    "manhole": [105, 105, 105],         # Dim gray (urban infrastructure)
    "pedestrian": [255, 182, 193],      # Light pink (human-centered spaces)
    "pier": [70, 130, 180],             # Steel blue (waterfronts)
    "power": [255, 69, 0],              # Bright orange-red (energy and intensity)
    "recreation": [144, 238, 144],      # Light green (parks and leisure)
    "tower": [128, 0, 0],               # Maroon (communication structures)
    "transit": [0, 128, 255],           # Bold royal blue (transport and networks)
    "utility": [0, 204, 204],           # Aqua cyan (essential services)
    "waste_management": [154, 205, 50], # Yellow-green (sustainability)
    "water": [0, 191, 255]              # Deep sky blue (aquatic environments)
}



