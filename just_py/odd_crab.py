@fused.udf
def udf(bounds: fused.types.Bounds = None):
    import pandas as pd
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    # convert bounds to tile
    tile = common.get_tiles(bounds, clip=True)
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    df = pd.DataFrame()
    scale = int((15 - z) * 1)
    print(scale)
