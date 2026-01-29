@fused.udf
def udf(bounds: fused.types.Tile,
        res : int = 11,
        path: str='s3://fused-users/stephenkentdata/DEM_from_USGS/stoddard/USGS_13_n44w073_20181204.tif', *, chip_len=256):
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    import numpy as np
    # Just read the raw data
    arr = utils.read_tiff(bounds, path, output_shape=(chip_len, chip_len))
    if arr is None:
        return
    # Convert to UInt16 to satisfy the PNG driver requirement
    arr = arr.astype(np.uint16)

    bounds = bounds.bounds.values[0]
    df = utils.arr_to_latlng(arr, bounds)
    dem_hex_utils = fused.load("https://github.com/fusedio/udfs/tree/0b1bd10/public/DEM_Tile_Hexify/").utils
    df = dem_hex_utils.aggregate_df_hex(df=df, res=res)
    df =df[df['metric']<4857]
    print(df)
    return df