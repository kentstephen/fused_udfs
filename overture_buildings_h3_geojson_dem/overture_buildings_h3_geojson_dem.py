# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
        bounds: fused.types.Bounds= [-126.6,20.9,-64.9,50.4], 
        res: int =6,
        time_of_interest="2023-08-20/2023-11-25",
        scale:float=0.099,
       ):
    import geopandas as gpd
    import pandas as pd
    # from utils import get_over, run_query
        # common = fused.load("https://github.com/fusedio/udfs/tree/main/public/common/")
    # common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    # zoom = common_utils.estimate_zoom(bounds)
    # tile = common.get_tiles(bounds, clip=True)

    res_offset = 0  # lower makes the hex finer
   
    print(res)
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds)
    tile_buffer = tile.copy()
    tile_buffer['geometry'] = tile.to_crs(tile.estimate_utm_crs()).buffer(1000).to_crs('EPSG:4326')
    # zoom = utils.estimate_zoom(bounds)
    # df_buildings = get_over(tile, overture_type="building")
  
    # if df_buildings is None or df_buildings.empty:
    #     return
    df_roads = get_over(tile=tile_buffer, overture_type="infrastructure")

    if df_roads is None or df_roads.empty:
        return
    # return df_roads
    # return df_roads
    # return df_r  oads
    # df_dem = fused.run("fsh_65CrKEyQM7ePE0X7PtzKBR", bounds=bounds, res=res) #USGS
    # df_dem = fused.run("fsh_2KKOTd6HSiGtNOYHLqG4xN", bounds=tile, res=res) # USGS dem_10meter_tile_hex_2
    # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) #hexify
    # df_sentinel = fused.run("fsh_5LQu6mVMYDT2NR4z7lKjXv", bounds=bounds, time_of_interest=time_of_interest, scale=scale, h3_size=res)
    # df_dem = fused.run("fsh_3M3RyItkeAZpGR6fMZ482r", bounds=bounds, res=res) # jaxa
    # if df_sentinel is None or df_sentinel.empty:
    #     return
    # print(df_sentinel)
    # print(df)
    # bounds = bounds.bounds.values[0]
    df_burn = fused.run("fsh_6sg5CMvW6eEtiUc4TEYdXd",bounds=bounds, res=res)
    if df_burn is None or len(df_burn)==0:
        return None
    df = run_query(df_roads, df_burn, res, tile=tile_buffer)
    df = gpd.clip(df, tile)
    # df['metric']= df['metric'] - 3000
    print(df.columns)
    return df
import pandas as pd
import shapely 
import geopandas as gpd

# @fused.cache
def get_over(tile, overture_type):

    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/")
    gdf = overture_maps.get_overture(bounds=tile, overture_type=overture_type, min_zoom=0)
    if gdf is None or gdf.empty or len(gdf)==0:
        return pd.DataFrame({})
    else:
 
        # if overture_type=="infrastructure":
            
        filter_classes = [
            'power_line',        # major transmission lines
            # 'minor_line',        # distribution lines
            # 'communication_line', # fiber/telecom lines
            # 'cable',             # undersea or major cables
            # 'pipeline',          # oil/gas/water mains (utility subtype, but significant)
        ]
                    # filter_types = ['power', 'transportation','communication', 'bridge']
        gdf = gdf[gdf['class'].isin(filter_classes)]
        gdf['voltage'] = gdf['source_tags'].apply(get_voltage)
        gdf = gdf[gdf['voltage'] >= 115000]
        if gdf is None or gdf.empty or len(gdf)==0:
            return pd.DataFrame({})
        # gdf = gpd.clip(gdf, tile)
        # gdf['geometry'] = gdf['geometry'].simplify(0.01)
        gdf['geometry_buffer'] = gdf['geometry']
        if gdf.geometry_buffer is not None:
            gdf['geometry_buffer']= gdf.to_crs(gdf.estimate_utm_crs()).buffer(25).to_crs('EPSG:4326')
        if gdf is None or gdf.empty or len(gdf)==0:
            return pd.DataFrame({})
        return gdf
    #         # gdf_cali = get_california_boundary()
    #         # if gdf_cali is None or len(gdf_cali)==0:
    #         #     return pd.DataFrame({})
    #         # gdf_cali = gpd.clip(gdf_cali, tile)
    #         # if gdf_cali is None or len(gdf_cali)==0:
    #         #     return pd.DataFrame({})
    #         # gdf = gpd.clip(gdf, gdf_cali)
    #         if gdf is None or len(gdf)==0:
    #             return None
    #         gdf['geometry_buffer'] = gdf['geometry']
    #         if gdf.geometry_buffer is not None:
    #             gdf['geometry_buffer']= gdf.to_crs(gdf.estimate_utm_crs()).buffer(5).to_crs('EPSG:4326')
    #         # gdf['geoemtry'] = gdf['geometry'].simplify(0.01)
    #         # gdf['geometry_buffer'] = gdf['geometry_buffer'].apply(shapely.wkt.dumps)
    #         # gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    #         return gdf #pd.DataFrame(gdf)
    #     if overture_type == "segment":
    #         walking_types = ['motorway', 'primary','secondary', 'tertiary']
    #         gdf = gdf[gdf['class'].isin(walking_types)]
            
    #         # Filter out rows with null geometries BEFORE estimating UTM
    #         # gdf = gdf[~gdf.geometry.isna()]
            
    #         # Now you can safely estimate UTM and buffer
    #         if gdf is not None and not gdf.empty:
    #             gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(25).to_crs('EPSG:4326')
    #         else:
    #             return pd.DataFrame({})  # Return empty DataFrame instead of None
                
    #         # Convert to WKT
    #         gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    #         return pd.DataFrame(gdf)
        
    #     elif overture_type == 'building':
    #         gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
    #         return pd.DataFrame(gdf)
        # elif overture_type == 'land_use':
        #     walking_types = ['national_park']
        #     gdf = gdf[gdf['class'].isin(walking_types)]
        #     if gdf is None or gdf.empty or len(gdf)==0:
        #         return pd.DataFrame({})
        #     # Filter out row
        #     # gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        #     return gdf # pd.DataFrame(gdf)
    # return gdf# pd.DataFrame(gdf)  # Default return for other overture_types
@fused.cache
def run_query(df_roads, df_burn, res, tile):
    import geopandas as gpd
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    # tile = get_tile(bounds, clip=True)
    xmin, ymin, xmax, ymax = tile.geometry.iloc[0].bounds
    # xmin, ymin, xmax, ymax = bounds
    # utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
   
    con = common.duckdb_connect()
    # df_roads = fused.run("fsh_5t5PNBeRWIGulM91JBO8GK")
    # df_roads = gpd.clip(df_roads, tile)
    df_roads_arrow = df_roads.to_arrow()
    con.sql("CALL register_geoarrow_extensions()")
    # res =con.sql("(select h3_get_resolution(hex)::int from df_burn limit 1)").fetchone()[0]
    query=f"""

WITH roads_to_cells AS (
    SELECT
        unnest(h3_polygon_wkt_to_cells_experimental(st_astext(geometry_buffer), {res}, 'overlap')) AS hex,
        id,
        st_astext(geometry) as geom_wkt,
        subtype,
        class,
        names.primary as name,
    FROM df_roads_arrow
)
SELECT

mode(r.name) as name,
mode(r.class) as class,
mode(r.subtype) as subtype,
max(b.burn_prob) * 100  as burn_prob,
r.geom_wkt
FROM roads_to_cells r
left join df_burn b on r.hex = b.hex

--where
  --  h3_cell_to_lat(b.hex) >= {ymin}
    --AND h3_cell_to_lat(b.hex) < {ymax}
    --AND h3_cell_to_lng(b.hex) >= {xmin}
    ---AND h3_cell_to_lng(b.hex) < {xmax}
group by geom_wkt
--group by all
    """
    df = con.sql(query).df()
    return gpd.GeoDataFrame(df.drop(columns=['geom_wkt']), geometry=df.geom_wkt.apply(shapely.wkt.loads))
@fused.cache
def get_california_boundary():
    """Load and dissolve California census tracts into single boundary."""
    import geopandas as gpd

    print("Loading California tracts...")
    gdf = gpd.read_file(
        "https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/06_CALIFORNIA/06/tl_rd22_06_tract.zip"
    )
    gdf = gdf.dissolve().to_crs(4326)
    # gdf.geometry = gdf.geometry.simplify(0.01)  # simplify for speed
    print("California boundary created")
    return gdf
@fused.cache
def get_voltage(source_tags_str):
    import geopandas as gpd
    import re
    import pandas as pd
    # Handle None, NaN, or empty values
    if source_tags_str is None:
        return None
    if isinstance(source_tags_str, float):  # catches NaN
        return None
    
    source_tags_str = str(source_tags_str)
    match = re.search(r"\('voltage',\s*'(\d+)'\)", source_tags_str)
    if match:
        return int(match.group(1))
    return None

# aerialway,
# airport,
# barrier
# bridge
# communication
# emergency
# manhole
# pedestrian
# pier
# power
# quay
# recreation
# tower
# transit
# transportation
# utility
# waste_management
# water

# {
#   "tileLayer": {
#     "@@type": "TileLayer",
#     "minZoom": 0,
#     "maxZoom": 19,
#     "tileSize": 256
#   },
#   "vectorLayer": {
#     "@@type": "GeoJsonLayer",
#     "stroked": true,
#     "filled": false,
#     "pickable": true,
#     "extruded": false,
#     "opacity": 1,
#     "coverage": 1,
#     "lineWidthMinPixels": 1,
#     "getHexagon": "@@=properties.hex",
#     "getLineColor": {
#       "@@function": "colorCategories",
#       "attr": "subtype",
#       "domain": [
#         "aerialway",
#         "airport",
#         "barrier",
#         "bridge",
#         "communication",
#         "emergency",
#         "manhole",
#         "pedestrian",
#         "pier",
#         "power",
#         "quay",
#         "recreation",
#         "tower",
#         "transit",
#         "transportation",
#         "utility",
#         "waste_management",
#         "water"
#       ],
#       "steps": 10,
#       "colors": "Bold",
#       "nullColor": [
#         184,
#         184,
#         184
#       ]
#     },
#     "getLneColor": [
#       200,
#       250,
#       0
#     ],
#     "getFilColor": [
#       184,
#       184,
#       184
#     ],
#     "getElevation": {
#       "@@function": "hasProp",
#       "property": "value",
#       "present": "@@=properties.value",
#       "absent": 1
#     },
#     "elevationScale": 10
#   }
# }
