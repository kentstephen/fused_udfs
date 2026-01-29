@fused.udf
def udf(bounds: fused.types.Bounds =None):
    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)

    return tile
