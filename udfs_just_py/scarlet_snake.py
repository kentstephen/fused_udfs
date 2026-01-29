# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, buffer_multiple: float = 1):
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/c3ab13d/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tiles = common.get_tiles(bounds, target_num_tiles=16)
    # Buffering tiles internally
    res_offet= 0
    res = max(min(int(3 + zoom / 1.5), 13) - res_offset, 2)
    df_veg = fused.run("fsh_6tYnDASQFlVcgRmFFsKfWY", bounds=bounds, res=res)
    df_cdl = fused.run("CDL_from_source_coop", crop_value_list= [54] ,   cell_to_parent_res: int =8)
    def run_query(df_veg, df_cdl):
        con = common.duckdb_connect()
        query="""select
        
        from df_veg v inner join df_cdl c on df_veg.hex"""
    
    
    
    
