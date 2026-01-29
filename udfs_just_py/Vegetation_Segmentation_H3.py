@fused.udf
def udf(bounds: fused.types.Bounds = None,
        res:int=12):
    import geopandas as gpd
    import shapely
    import xarray as xr
    from utils import df_to_hex
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/482f6de/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    # Set the resolution to be based on zoom.
    res_offset = 0  # lower makes the hex finer
    # resolution = max(min(int(3 + zoom / 1.5), 13) - res_offset, 2)\
    #
    # print(resolution)
    
    # Load the Vegetation Segmentation xarray dataset.
    arr = fused.run("UDF_Vegetation_Segmentation", bounds=bounds)
    # print(ds)
    print(arr)
    
    # Select band 4.
    # arr = ds.values
    # print(arr)
    bounds_gdf = common_utils.bounds_to_gdf(bounds)
    bounds_values = bounds_gdf.bounds.values[0]
    # Load and run common function arr_to_latlng
    df_arr = common_utils.arr_to_latlng(arr[1], bounds_values) 
    print(df_arr)
    
    # Turn the values into H3.
    df = df_to_hex(df=df_arr ,bounds_values=bounds_values, res=res)
    # This seems to help with the tile distortion.
    # df['hex'] = df['hex'].drop_duplicates()
    
    print(df)
    return df