@fused.udf
def udf(bounds: fused.types.Bounds,
        res: int=14):
    import geopandas as gpd
    import shapely
    import pandas as pd
    # import mesa
    
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    res_offset =0  # lower makes the hex finer
    # res = max(min(int(3 + zoom / 1.5), 12) - res_offset, 2)
    # res=13
    print(res)
    utils = fused.load("https://github.com/fusedio/udfs/tree/e74035a1/public/common/").utils
    bounds = utils.bounds_to_gdf(bounds)
    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/")
    gdf = overture_maps.get_overture(bounds=tile, overture_type='segment', min_zoom=0)
    if len(gdf) < 1:
        return
    elif gdf is None or gdf.empty:
        return
    # Filter by walking types
    # walking_types = ['motorway']
    # gdf = gdf[gdf['class'].isin(walking_types)]
    
    # Filter out rows with null geometries BEFORE estimating UTM
    # gdf = gdf[~gdf.geometry.isna()]
    
    # Now you can safely estimate UTM and buffer
    if gdf is not None and not gdf.empty:
       gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(2).to_crs('EPSG:4326')
    else:
        return
    # Convert to WKT
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
            
    # print(gdf['class'])
    # df_dem = fused.run("fsh_2bT8AoAunIsV5g7Rj4SC1B", x=x, y=y, z=z)
    df_overture = pd.DataFrame(gdf)
    # print(df_overture)
    # df_dem = fused.run("fsh_65CrKEyQM7ePE0X7PtzKBR", bounds=bounds, res=res) # USGS
    df_dem = fused.run("fsh_8G9Hs7bL4VS0QEiSFJKbU", bounds=tile, res=res) # 1m stoddard usgs
    # print(df_dem.columns)
    def get_cells(df_overture, df_dem, resolution):
        utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
        ).utils
        con = utils.duckdb_connect()
        query = f"""
with to_cells as (
select
  unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'center')) AS hex
  from df_overture
) 
select 
  t.hex,
  d.metric,
  
from to_cells t 
inner join df_dem d on t.hex = d.hex
group by all



        """
        # Run the query and return a GeoDataFrame
        return con.sql(query).df()
    df = get_cells(df_overture, df_dem, resolution=res)

    print(df)
    return df    
# return gdf_overture