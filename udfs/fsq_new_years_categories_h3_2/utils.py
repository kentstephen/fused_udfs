CMAP = {
    "Health and Medicine": [102, 194, 165],      # Soft teal for soothing clarity
    "Arts and Entertainment": [252, 141, 98],   # Warm coral for vibrancy and creativity
    "Landmarks and Outdoors": [141, 160, 203],  # Calming lavender for connection with nature
    "Travel and Transportation": [166, 216, 84],# Fresh green for movement and growth
    "Sports and Recreation": [255, 217, 47]     # Bold golden yellow for activity and energy
}



import pandas as pd
def add_rgb_cmap(gdf, key_field, cmap_dict):
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