# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, buffer_multiple: float = 1):
    # Using common fused functions as helper
    res = 14
    df_nlcd = fused.run("fsh_7AJES4TkAoyT3E0pfuwBZl",bounds=bounds, res=res)
    print(df_nlcd)