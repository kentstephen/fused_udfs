# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None,
        chip_len:int=512, scale:float=0.1,
         time_of_interest = "2021--01/2021-12-30",
       ):
    import numpy as np
    from utils import get_arr
    # Using common fused functions as helper
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/2f41ae1/public/common/").utils
    tile = common_utils.get_tiles(bounds, clip=True)
    udf_sentinel = fused.load('https://github.com/fusedio/udfs/tree/a1c01c6/public/DC_AOI_Example/')
    arr = get_arr(tile, time_of_interest=time_of_interest, output_shape=(chip_len, chip_len), max_items=50)
    arr = np.clip(arr *  scale, 0, 255).astype("uint8")[:3]
    # arr = arr[:3] 
    
    return arr