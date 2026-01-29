# utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds=None,
        res: int =14,
        stats_type: str = "mean"
):
    # utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    import rasterio
    
    import rasterio

    url = "https://granit24a.sr.unh.edu/image/rest/services/ImageServices/LiDAR_Bare_Earth_DEM_NH_2022/ImageServer"
    
    with rasterio.open(url) as src:
        print(f"CRS: {src.crs}")
        print(f"Bounds: {src.bounds}")
        print(f"Resolution: {src.res}")
        print(f"Shape: {src.shape}")
        
        # Read a window for your AOI
        # window = src.window(minx, miny, maxx, maxy)
        # data = src.read(1, window=window)