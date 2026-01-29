import pandas as pd
import pyarrow as pa
import duckdb
@fused.cache
def get_data():
    df = pd.read_csv("https://data.cityofnewyork.us/api/views/5rq2-4hqu/rows.csv?accessType=DOWNLOAD")
    df["latitude"] = df["Latitude"]
    df["name"] = df["spc_common"]
    table = pa.Table.from_pandas(df)
    return table
    
    return df
def get_con():
    
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
                LOAD h3ext;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;""")
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

CMAP = {
    "sycamore maple": [255, 99, 132],
    "pignut hickory": [54, 162, 235],
    "green ash": [255, 206, 86],
    "Norway spruce": [75, 192, 192],
    "Himalayan cedar": [153, 102, 255],
    "shingle oak": [255, 159, 64],
    "American beech": [201, 203, 207],
    "pond cypress": [0, 255, 0],
    "cockspur hawthorn": [255, 0, 0],
    "cucumber magnolia": [0, 128, 0],
    "Scots pine": [255, 20, 147],
    "mimosa": [139, 0, 139],
    "pin oak": [75, 0, 130],
    "American elm": [70, 130, 180],
    "Turkish hazelnut": [240, 230, 140],
    "Atlantic white cedar": [144, 238, 144],
    "Douglas-fir": [255, 105, 180],
    "sassafras": [106, 90, 205],
    "Chinese elm": [210, 105, 30],
    "holly": [220, 20, 60],
    "white ash": [255, 255, 0],
    "tree of heaven": [255, 69, 0],
    "empress tree": [0, 0, 139],
    "Amur cork tree": [173, 255, 47],
    "dawn redwood": [46, 139, 87],
    "flowering dogwood": [255, 20, 147],
    "eastern redbud": [153, 50, 204],
    "red horse chestnut": [147, 112, 219],
    "Shantung maple": [255, 127, 80],
    "trident maple": [0, 255, 255],
    "pine": [0, 191, 255],
    "pitch pine": [255, 0, 255],
    "red maple": [0, 255, 127],
    "willow oak": [238, 130, 238],
    "Amur maple": [205, 133, 63],
    "sawtooth oak": [255, 0, 0],
    "southern magnolia": [255, 215, 0],
    "Callery pear": [0, 250, 154],
    "mulberry": [184, 134, 11],
    "spruce": [154, 205, 50],
    "cherry": [123, 104, 238],
    "Schumard's oak": [64, 224, 208],
    "Siberian elm": [255, 165, 0],
    "Kentucky coffeetree": [127, 255, 212],
    "Oklahoma redbud": [72, 61, 139],
    "boxelder": [50, 205, 50],
    "European beech": [199, 21, 133],
    "Japanese maple": [0, 0, 205],
    "smoketree": [255, 69, 0],
    "paper birch": [255, 105, 180],
    "American larch": [102, 205, 170],
    "Osage-orange": [173, 216, 230],
    "honeylocust": [0, 139, 139],
    "ash": [0, 0, 128],
    "silver maple": [255, 228, 181],
    "black cherry": [70, 130, 180],
    "sweetgum": [139, 69, 19],
    "silver linden": [205, 92, 92],
    "English oak": [255, 99, 71],
    "southern red oak": [160, 82, 45],
    "Chinese tree lilac": [139, 0, 0],
    "eastern cottonwood": [85, 107, 47],
    "bald cypress": [210, 180, 140],
    "red pine": [244, 164, 96],
    "tartar maple": [72, 209, 204],
    "Amur maackia": [105, 105, 105],
    "European hornbeam": [112, 128, 144],
    "crab apple": [0, 191, 255],
    "Norway maple": [255, 160, 122],
    "scarlet oak": [173, 255, 47],
    "Japanese zelkova": [255, 0, 255],
    "horse chestnut": [147, 112, 219],
    "littleleaf linden": [60, 179, 113],
    "white pine": [100, 149, 237],
    "Japanese tree lilac": [0, 206, 209],
    "katsura tree": [72, 61, 139],
    "black locust": [250, 128, 114],
    "Persian ironwood": [124, 252, 0],
    "Chinese chestnut": [199, 21, 133],
    "crimson king maple": [210, 105, 30],
    "arborvitae": [0, 128, 128],
    "maple": [255, 69, 0],
    "paperbark maple": [255, 99, 71],
    "European alder": [85, 107, 47],
    "Virginia pine": [144, 238, 144],
    "pagoda dogwood": [240, 128, 128],
    "hedge maple": [238, 130, 238],
    "Ohio buckeye": [50, 205, 50],
    "Kentucky yellowwood": [139, 0, 139],
    "crepe myrtle": [0, 255, 0],
    "golden raintree": [255, 20, 147],
    "weeping willow": [255, 165, 0],
    "Cornelian cherry": [255, 99, 71],
    "silver birch": [0, 255, 255],
    "'Schubert' chokecherry": [0, 0, 128],
    "catalpa": [139, 0, 0],
    "river birch": [218, 112, 214],
    "blue spruce": [75, 0, 130],
    "black pine": [72, 61, 139],
    "bur oak": [255, 69, 0],
    "ginkgo": [0, 255, 0],
    "Sophora": [255, 0, 0],
    "swamp white oak": [255, 140, 0],
    "black oak": [205, 92, 92],
    "northern red oak": [70, 130, 180],
    "blackgum": [0, 128, 128],
    "purple-leaf plum": [154, 205, 50],
    "sugar maple": [176, 224, 230],
    "Atlas cedar": [124, 252, 0],
    "serviceberry": [255, 69, 0],
    "black maple": [255, 99, 71],
    "quaking aspen": [255, 215, 0],
    "false cypress": [255, 105, 180],
    "American linden": [72, 209, 204],
    "London planetree": [50, 205, 50],
    "eastern redcedar": [123, 104, 238],
    "tulip-poplar": [0, 206, 209],
    "Chinese fringetree": [0, 191, 255],
    "white oak": [105, 105, 105],
    "hardy rubber tree": [154, 205, 50],
    "black walnut": [139, 69, 19],
    "Japanese snowbell": [255, 127, 80],
    "American hornbeam": [75, 0, 130],
    "eastern hemlock": [0, 100, 0],
    "hawthorn": [139, 0, 0],
    "magnolia": [233, 150, 122],
    "American hophornbeam": [160, 82, 45],
    "common hackberry": [0, 139, 139],
    "kousa dogwood": [0, 255, 127],
    "bigtooth aspen": [255, 140, 0],
    "two-winged silverbell": [100, 149, 237],
    "Japanese hornbeam": [70, 130, 180]
}

