# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, resolution:int=10, min_count:int=7):
    # Using common fused functions as helper
    utils = fused.load("https://github.com/fusedio/udfs/tree/3569595/public/common/").utils
    gdf_cab = fused.run("fsh_140FuXAFreaz3J3lMUf6h4",
            resolution=resolution, 
            min_count=min_count
        )
    @fused.cache
    def get_overture(bounds):
         return fused.run(
            "UDF_Overture_Maps_Example",
         
            overture_type="building",
           bounds=bounds,
            min_zoom=0
        )
    gdf_overture = get_overture(bounds=bounds)
    # Join the two dataframes.
    gdf_joined = gdf_overture.sjoin(gdf_cab)
    
    print(gdf_joined)
    return gdf_joined
