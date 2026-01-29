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

cmap = {
    "Other": [255, 0, 0],  # Red
    "NULL": [128, 128, 128],  # Grey
    "Arctic, Alpine natural vegetation": [0, 255, 255],  # Cyan
    "Plantation and cultivated land vegetation": [0, 128, 0],  # Dark Green
    "Subarctic, Subalpine natural vegetation": [0, 0, 255],  # Blue
    "Camellia class natural vegetation": [255, 165, 0],  # Orange
    "Riparian, wetland, salt marsh, and dune vegetation": [0, 255, 0],  # Green
    "Alpine natural vegetation area": [255, 20, 147],  # Pink
    "Lingonberry - Spruce class natural vegetation": [255, 215, 0],  # Gold
    "Riparian, wetland, salt marsh, dune vegetation, etc.": [75, 0, 130],  # Indigo
    "Lingonberry - Spruce class replacement vegetation": [138, 43, 226],  # Blue Violet
    "Beech class natural vegetation": [34, 139, 34],  # Forest Green
    "Beech class replacement vegetation": [255, 140, 0],  # Dark Orange
    "Subarctic, Subalpine replacement vegetation": [160, 82, 45],  # Saddle Brown
    "Camellia class replacement vegetation": [70, 130, 180]  # Steel Blue
}
