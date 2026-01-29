@fused.udf#(cache_max_age="0s")
def udf(bounds: fused.types.Bounds = None, resolution: int = 12):
    import pandas as pd
    import shapely
    common = fused.load("https://github.com/fusedio/udfs/tree/6dd2c4e/public/common/")
    tile = common.get_tiles(bounds, clip=True)
    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/")

   # con.execute("")
    
    # Get your data
    gdf = overture_maps.get_overture(bounds=tile, min_zoom=0)
    # gdf = gdf[gdf['level'].notna() & (gdf['level'] >= 1)]
    gdf = gdf[gdf.get('is_underground', False) != True]
    # Remove CRS if you're on DuckDB < 1.4.1 (otherwise skip this line)
    # gdf = gdf.set_crs(None)
    
    # Convert GeoDataFrame to Arrow
    gdf_arrow = gdf.to_arrow()
    con = common.duckdb_connect()
    # Now DuckDB can query it directly
    # df_o = con.sql("SELECT * FROM gdf_arrow").df()
   
    con.sql("CALL register_geoarrow_extensions()")
    qr =f"""  
    
    
    select
unnest(h3_polygon_wkt_to_cells_experimental(st_astext(geometry), {resolution}, 'full')) as hex,
from gdf_arrow;
    """
    df = con.sql(qr).df()
    return df