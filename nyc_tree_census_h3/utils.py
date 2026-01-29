import pandas as pd
def get_con():
    import duckdb
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
                LOAD h3ext;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
               -- SET s3_region='us-west-2';""")
    return con
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

"""NWS Hazard Type Colormap"""
CMAP = {
    "sycamore maple": [34, 139, 34],
    "pignut hickory": [255, 69, 0],
    "green ash": [139, 0, 0],
    "Norway spruce": [0, 100, 0],
    "Himalayan cedar": [0, 128, 128],
    "shingle oak": [165, 42, 42],
    "American beech": [128, 0, 0],
    "pond cypress": [0, 255, 127],
    "cockspur hawthorn": [218, 165, 32],
    "cucumber magnolia": [255, 140, 0],
    "Scots pine": [85, 107, 47],
    "mimosa": [154, 205, 50],
    "pin oak": [143, 188, 143],
    "American elm": [47, 79, 79],
    "Turkish hazelnut": [160, 82, 45],
    "Atlantic white cedar": [72, 61, 139],
    "Douglas-fir": [0, 128, 0],
    "sassafras": [173, 255, 47],
    "Chinese elm": [124, 252, 0],
    "holly": [34, 139, 34],
    "white ash": [139, 69, 19],
    "tree of heaven": [70, 130, 180],
    "empress tree": [176, 224, 230],
    "Amur cork tree": [32, 178, 170],
    "dawn redwood": [222, 184, 135],
    "flowering dogwood": [255, 105, 180],
    "eastern redbud": [238, 130, 238],
    "red horse chestnut": [221, 160, 221],
    "Shantung maple": [255, 99, 71],
    "trident maple": [255, 69, 0],
    "pine": [34, 139, 34],
    "pitch pine": [0, 100, 0],
    "red maple": [255, 0, 0],
    "willow oak": [50, 205, 50],
    "Amur maple": [255, 20, 147],
    "sawtooth oak": [218, 112, 214],
    "southern magnolia": [250, 128, 114],
    "Callery pear": [255, 215, 0],
    "mulberry": [107, 142, 35],
    "spruce": [46, 139, 87],
    "cherry": [205, 92, 92],
    "Schumard's oak": [75, 0, 130],
    "Siberian elm": [0, 0, 255],
    "Kentucky coffeetree": [65, 105, 225],
    "Oklahoma redbud": [138, 43, 226],
    "boxelder": [72, 61, 139],
    "European beech": [75, 0, 130],
    "Japanese maple": [153, 50, 204],
    "smoketree": [186, 85, 211],
    "paper birch": [147, 112, 219],
    "American larch": [139, 0, 139],
    "Osage-orange": [255, 0, 255],
    "honeylocust": [238, 130, 238],
    "ash": [255, 20, 147],
    "silver maple": [255, 105, 180],
    "black cherry": [139, 69, 19],
    "sweetgum": [205, 92, 92],
    "silver linden": [233, 150, 122],
    "English oak": [255, 140, 0],
    "southern red oak": [255, 127, 80],
    "Chinese tree lilac": [255, 99, 71],
    "eastern cottonwood": [255, 69, 0],
    "bald cypress": [210, 105, 30],
    "red pine": [255, 165, 0],
    "tartar maple": [255, 215, 0],
    "Amur maackia": [184, 134, 11],
    "European hornbeam": [218, 165, 32],
    "crab apple": [255, 140, 0],
    "Norway maple": [255, 215, 0],
    "scarlet oak": [255, 69, 0],
    "Japanese zelkova": [255, 99, 71],
    "horse chestnut": [255, 127, 80],
    "littleleaf linden": [255, 140, 0],
    "white pine": [0, 100, 0],
    "Japanese tree lilac": [124, 252, 0],
    "katsura tree": [0, 255, 127],
    "black locust": [34, 139, 34],
    "Persian ironwood": [0, 128, 128],
    "Chinese chestnut": [210, 105, 30],
    "crimson king maple": [255, 0, 0],
    "arborvitae": [0, 128, 0],
    "maple": [255, 0, 0],
    "paperbark maple": [255, 69, 0],
    "European alder": [34, 139, 34],
    "Virginia pine": [0, 100, 0],
    "pagoda dogwood": [255, 140, 0],
    "hedge maple": [255, 215, 0],
    "Ohio buckeye": [255, 127, 80],
    "Kentucky yellowwood": [218, 165, 32],
    "crepe myrtle": [255, 69, 0],
    "golden raintree": [255, 215, 0],
    "weeping willow": [124, 252, 0],
    "Cornelian cherry": [255, 105, 180],
    "silver birch": [205, 92, 92],
    "'Schubert' chokecherry": [139, 69, 19],
    "catalpa": [205, 133, 63],
    "river birch": [139, 0, 0],
    "blue spruce": [70, 130, 180],
    "black pine": [0, 100, 0],
    "bur oak": [85, 107, 47],
    "ginkgo": [173, 255, 47],
    "Sophora": [154, 205, 50],
    "swamp white oak": [143, 188, 143],
    "black oak": [47, 79, 79],
    "northern red oak": [255, 0, 0],
    "blackgum": [128, 0, 0],
    "purple-leaf plum": [238, 130, 238],
    "sugar maple": [255, 69, 0],
    "Atlas cedar": [0, 128, 0],
    "serviceberry": [176, 224, 230],
    "black maple": [255, 0, 0],
    "quaking aspen": [0, 128, 128],
    "false cypress": [210, 105, 30],
    "American linden": [255, 99, 71],
    "London planetree": [255, 127, 80],
    "eastern redcedar": [0, 255, 127],
    "tulip-poplar": [34, 139, 34],
    "Chinese fringetree": [139, 69, 19],
    "white oak": [255, 255, 255],
    "hardy rubber tree": [240, 230, 140],
    "black walnut": [128, 0, 0],
    "Japanese snowbell": [255, 105, 180],
    "American hornbeam": [34, 139, 34],
    "eastern hemlock": [0, 100, 0],
    "hawthorn": [255, 0, 0],
    "magnolia": [255, 215, 0],
    "American hophornbeam": [139, 69, 19],
    "common hackberry": [210, 105, 30],
    "kousa dogwood": [255, 140, 0],
    "bigtooth aspen": [173, 255, 47],
    "two-winged silverbell": [154, 205, 50],
    "Japanese hornbeam": [47, 79, 79]

}