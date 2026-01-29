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
