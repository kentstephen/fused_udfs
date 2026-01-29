common = fused.load("https://github.com/fusedio/udfs/tree/b3a7ff8/public/common/").utils
nlcd_example_utils = fused.load('https://github.com/fusedio/udfs/tree/1b2b7e3/public/NLCD_Tile_Example/').utils
common_utils = fused.load("https://github.com/fusedio/udfs/tree/36f4e97/public/common/").utils


@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    year: int = 2024,
    crop_type: str = "",
    chip_len: int = 256,
    colored: bool = True,
    res: int= 11,
    
):
    """"""
    import numpy as np
    import pandas as pd

    # convert bounds to tile
    tile = common.get_tiles(bounds, clip=True)

    input_tiff_path = f"s3://fused-asset/data/cdls/{year}_30m_cdls.tif"
    data = common.read_tiff(
        tile, input_tiff_path, output_shape=(chip_len, chip_len), return_colormap=True, cache_max_age="90d"
    )
    if data:
        arr_int, metadata = data
    else:
        print("no data")
        return None
    # if crop_type:
    
    arr_int = filter_crops(arr_int, crop_type, verbose=False)

    # Print out the top 20 classes
    print(crop_counts(arr_int).head(20))
    colormap = metadata["colormap"]
    colored_array = (
        np.array([colormap[value] for value in arr_int.flat], dtype=np.uint8)
        .reshape(arr_int.shape + (4,))
        .transpose(2, 0, 1)
    )

    if colored:
        df = common_utils.arr_to_h3(arr_int, bounds, res=res, ordered=False)

        # find most frequet land_type
        df['most_freq'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[0][np.argmax(np.unique(x, return_counts=True)[1])])
        df['n_pixel'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[1].max())
        df=df[df['most_freq']>0]
        if len(df)==0:
            print(f"Empty dataframe for tile {tile}")
            return
    
        # get the color and land_type
        df[['r', 'g', 'b', 'a']] = df.most_freq.map(lambda x: pd.Series(colormap[x])).apply(pd.Series)
        df['crop_type'] = df.most_freq.map(int_to_crop)
        df['color'] = df.most_freq.map(lambda x: nlcd_example_utils.rgb_to_hex(colormap[x]) if x in colormap else "NaN")
        df=df[['hex', 'crop_type', 'color', 'r', 'g', 'b', 'a', 'most_freq','n_pixel']]

        return df
    else:
        return array_int, bounds


@fused.cache
def crop_to_int(crop_type, verbose=True):
    import pandas as pd
    import requests

    url = "https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json"
    df_meta = pd.DataFrame(requests.get(url).json()["summaries"]["eo:bands"][0]["gee:classes"])
    mask = df_meta.description.map(lambda x: crop_type.lower() in x.lower())
    df_meta_masked = df_meta[mask]
    if verbose:
        print(f"{df_meta_masked=}")
        print("crop type options", df_meta.description.values)
    return df_meta_masked.value.values


@fused.cache
def int_to_crop(val):
    import requests
    import pandas as pd

    url = "https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json"
    df_meta = pd.DataFrame(requests.get(url).json()["summaries"]["eo:bands"][0]["gee:classes"]).set_index("value")
    return df_meta.loc[val].description


@fused.cache
def crop_type_list(crop_type):
    CDL = fused.load("UDF_CDLs_Tile_Example")
    try:
        vals = [int(i) for i in crop_type.split(',')]
    except:
        vals = CDL.crop_to_int(crop_type, verbose=False)
    return vals

def filter_crops(arr, crop_type, verbose=True):
    import numpy as np
    
    values_to_keep = crop_type_list(crop_type)
    if len(values_to_keep) > 0:
        mask = np.isin(arr, values_to_keep, invert=True)
        arr[mask] = 0
        return arr
    else:
        print(f"{crop_type=} was not found in the list of crops")
        return arr


def crop_counts(arr):
    import numpy as np
    import pandas as pd
    import requests

    unique_values, counts = np.unique(arr, return_counts=True)
    df = pd.DataFrame(counts, index=unique_values, columns=["n_pixel"]).sort_values(by="n_pixel", ascending=False)
    url = "https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json"
    df_meta = pd.DataFrame(requests.get(url).json()["summaries"]["eo:bands"][0]["gee:classes"])
    df = df_meta.set_index("value").join(df)[["description", "color", "n_pixel"]]
    df["color"] = "#" + df["color"]
    df.columns = ["crop_type", "color", "n_pixel"]
    df = df.sort_values("n_pixel", ascending=False)
    return df.dropna()

def crop_stats(df, n=100):
    stats = (
        df.groupby("data")
        .area.sum()
        .map(lambda x: round(x * 0.000247105, 2)) #conveting m2 to acre
        .sort_values(ascending=False)
        .to_frame("area (Acre)")
    )
    stats["name"] = stats.index.map(int_to_crop)
    return stats.head(n)
