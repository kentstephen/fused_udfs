# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds,    
        time_of_interest="2025-03-25/2025-06-10", 
        chip_len:int=512, 
        scale:float=0.2,):
    import json
    import numpy as np
    sentinel_params = json.dumps(
    {
        "collection": "sentinel-2-l2a",
        "band_list": ["B02", "B03", "B04", "B08"],
        "time_of_interest": time_of_interest,
        "query": {"eo:cloud_cover": {"lt": 5}},
        "scale": scale,
    }
)
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/b54f05a/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tile = common.get_tiles(bounds)
    # Buffering tiles internally
    # tiles.geometry = tiles.buffer(buffer_multiple / (tiles.z.iloc[0]) ** 2)
    # Use print statements to debug
    arr = fused.run("UDF_Satellite_Greenest_Pixel", bounds=tile, sentinel_params=sentinel_params)
    arr_scaled = arr.astype("uint8")
    # arr_scaled = np.clip(arr_scaled, 0, 255).astype("uint8")
    return arr_scaled
    # TODO: color correctio
    return arr