@fused.udf
def udf(bounds: fused.types.Bounds = None):
    import pandas as pd

    import pandas as pd
    import shapely
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    gdf = common.table_to_tile(table='s3://fused-users/stephenkentdata/stephenkentdata/fiboa/USDA_NationalCSB_2017-2024_rev23/',
                               bounds=bounds,
                               use_columns=['CSBID', 'CSBYEARS', 'CSBACRES', 'CDL2017', 'CDL2018', 'CDL2019',
       'CDL2020', 'CDL2021', 'CDL2022', 'CDL2023', 'CDL2024', 'STATEFIPS',
       'STATEASD', 'ASD', 'CNTY', 'CNTYFIPS', 'INSIDE_X', 'INSIDE_Y',
       'Shape_Length', 'Shape_Area'],
                               min_zoom=0)
    return gdf