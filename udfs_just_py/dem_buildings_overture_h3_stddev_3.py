@fused.udf
def udf(bounds: fused.types.Bounds,
        res: int=12):
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
    
    print(res)
    utils = fused.load("https://github.com/fusedio/udfs/tree/e74035a1/public/common/").utils
    bounds = utils.bounds_to_gdf(bounds)
    gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=bounds, min_zoom=0)
    if len(gdf) < 1:
        return
    elif gdf is None or gdf.empty:
        return
    # Filter by walking types
    # walking_types = ['motorway','primary', 'secondary','tertiary']
    # gdf = gdf[gdf['class'].isin(walking_types)]
    
    # Filter out rows with null geometries BEFORE estimating UTM
    # gdf = gdf[~gdf.geometry.isna()]
    
    # Now you can safely estimate UTM and buffer
    # if gdf is not None and not gdf.empty:
    #    gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(15).to_crs('EPSG:4326')
    # else:
    #     return
    # Convert to WKT
    gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
            
    # x, y, z = tile.iloc[0][["x", "y", "z"]]
    # print(x, y, z)
    # df_dem = fused.run("fsh_2bT8AoAunIsV5g7Rj4SC1B", x=x, y=y, z=z)
    df_overture = pd.DataFrame(gdf)
    # print(df_overture)
    # df_dem = fused.run("fsh_2bT8AoAunIsV5g7Rj4SC1B", x=x, y=y, z=z)
    # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) # hexify
    df_dem = fused.run("fsh_65CrKEyQM7ePE0X7PtzKBR", bounds=bounds, res=res) # USGS
    # print(df_dem.columns)
    def get_cells(df_overture, df_dem, resolution):
        utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
        ).utils
        con = utils.duckdb_connect()
        query = f"""
with to_cells as (
  select
    unnest(h3_polygon_wkt_to_cells_experimental(geometry, {resolution}, 'center')) AS hex,
   - height
  from df_overture
), 
add_elevation as (
  select 
    t.hex,
    d.metric,
  --  avg(t.height) as height
  from to_cells t 
  inner join df_dem d on t.hex = d.hex
 
), 
to_grid as (
  select
    unnest(h3_grid_disk(hex, 10)) as disk_hex,
    metric
  from df_dem
)
select 
  a.hex,
  stddev(t.metric) as stddev,
  a.metric,
  
from add_elevation a
inner join to_grid t on t.disk_hex = a.hex
group by a.hex, a.metric



        """
        # Run the query and return a GeoDataFrame
        return con.sql(query).df()
    df = get_cells(df_overture, df_dem, resolution=res)
    
    # df['metric'] = df["metric"] - 1000 
    print(df)
    return df    
# return gdf_overture