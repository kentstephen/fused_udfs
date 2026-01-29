# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
        bounds: fused.types.Bounds= [-126.6,20.9,-64.9,50.4], 
        res: int =11,
        time_of_interest="2023-08-20/2023-11-25",
        scale:float=0.099,
       ):
    import geopandas as gpd
    import pandas as pd
    # from utils import get_over, run_query
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    res_offset = 0  # lower makes the hex finer
   
    print(res)
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)
    # zoom = utils.estimate_zoom(bounds)
    # df_buildings = get_over(tile, overture_type="building")
  
    # if df_buildings is None or df_buildings.empty:
    #     return
    df_roads = get_over(tile, overture_type="infrastructure")
    if df_roads is None or df_roads.empty:
        return
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
    df = run_query(df_roads, res, tile)
    # df['metric']= df['metric'] - 3000
    print(df.columns)
    return df
import pandas as pd
import shapely 
import geopandas as gpd
def get_over(tile, overture_type):
    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/")
    gdf = overture_maps.get_overture(bounds=tile, overture_type=overture_type, min_zoom=0)
    if gdf is None or gdf.empty or len(gdf)<0:
        return pd.DataFrame({})
    else:
 
        if overture_type=="infrastructure":
            # gdf['geometry'] = gdf['geometry'].simplify(0.01)
            gdf = gdf[gdf.geometry.geom_type == 'LineString']
            if gdf is None or gdf.empty or len(gdf)<0:
                return pd.DataFrame({})
            gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(5).to_crs('EPSG:4326')
            # gdf['geoemtry'] = gdf['geometry'].simplify(0.01)
            gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
            return pd.DataFrame(gdf)
        if overture_type == "segment":
            walking_types = ['motorway', 'primary','secondary', 'tertiary']
            gdf = gdf[gdf['class'].isin(walking_types)]
            
            # Filter out rows with null geometries BEFORE estimating UTM
            # gdf = gdf[~gdf.geometry.isna()]
            
            # Now you can safely estimate UTM and buffer
            if gdf is not None and not gdf.empty:
                gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(15).to_crs('EPSG:4326')
            else:
                return pd.DataFrame({})  # Return empty DataFrame instead of None
                
            # Convert to WKT
            gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
            return pd.DataFrame(gdf)
        
        elif overture_type == 'building':
            gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
            return pd.DataFrame(gdf)
        elif overture_type == 'land_use':
            gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
            return pd.DataFrame(gdf)
    return pd.DataFrame(gdf)  # Default return for other overture_types
# @fused.cache
def run_query(df_roads, res, tile):
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    # tile = get_tile(bounds, clip=True)
    xmin, ymin, xmax, ymax = tile.geometry.iloc[0].bounds
    # xmin, ymin, xmax, ymax = bounds
    # utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = common.duckdb_connect()
    query=f"""

WITH roads_to_cells AS (
    SELECT
        unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'overlap')) AS hex,
        subtype
    FROM df_roads
)
SELECT  hex,
mode(subtype) as subtype,
count(1) as cnt
FROM roads_to_cells


where
    h3_cell_to_lat(hex) >= {ymin}
    AND h3_cell_to_lat(hex) < {ymax}
    AND h3_cell_to_lng(hex) >= {xmin}
    AND h3_cell_to_lng(hex) < {xmax}
group by 1
--group by all
    """
    return con.sql(query).df()
    # return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))

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
