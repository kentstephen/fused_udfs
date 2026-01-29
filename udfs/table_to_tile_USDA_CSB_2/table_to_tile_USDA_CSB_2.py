@fused.udf
def udf(bounds: fused.types.Bounds = None, path: str = "s3://fused-sample/demo_data/housing/housing_2024.csv"):
    import pandas as pd

    import pandas as pd
    import shapely
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    gdf = common.table_to_tile(table='s3://fused-users/stephenkentdata/stephenkentdata/fiboa/USDA/',bounds=bounds, use_columns=['geometry'],min_zoom=0)
    return gdf