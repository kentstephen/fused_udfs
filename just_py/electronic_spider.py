@fused.udf
def udf(bounds: fused.types.Bounds=None):
    import pandas as pd

    common = fused.load("UDF_common")
    
