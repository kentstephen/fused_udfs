# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
        bounds: fused.types.Bounds= [-126.6,20.9,-64.9,50.4], 
        res: int =8,
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
    tile = common.get_tiles(bounds, clip=True)
    # zoom = utils.estimate_zoom(bounds)
    # df_buildings = get_over(tile, overture_type="building")
  
    # if df_buildings is None or df_buildings.empty:
    #     return
    df_roads = get_over(bounds, overture_type="infrastructure")
    if df_roads is None or df_roads.empty:
        return
        
    return df_roads

import pandas as pd
import shapely 
import geopandas as gpd
@fused.cache
def get_over(bounds, overture_type):
    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/")
    gdf = overture_maps.get_overture(bounds=bounds, overture_type=overture_type, min_zoom=0)
    if gdf is None or gdf.empty or len(gdf)<0:
        return pd.DataFrame({})
    else:
 
        if overture_type=="infrastructure":
            
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
            # gdf['geometry'] = gdf['geometry'].simplify(0.01)
            
            if gdf is None or gdf.empty or len(gdf)==0:
                return pd.DataFrame({})
            # return gdf
            gdf_cali = get_california_boundary()
            if gdf_cali is None or len(gdf_cali)==0:
                return None
            gdf = gpd.clip(gdf, gdf_cali)
            if gdf is None or len(gdf)==0:
                return None
            gdf['geometry_buffer'] = gdf['geometry']
            if gdf.geometry_buffer is not None:
                gdf['geometry_buffer']= gdf.to_crs(gdf.estimate_utm_crs()).buffer(5).to_crs('EPSG:4326')
            # gdf['geoemtry'] = gdf['geometry'].simplify(0.01)
            # gdf['geometry_buffer'] = gdf['geometry_buffer'].apply(shapely.wkt.dumps)
            # gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
            return gdf #pd.DataFrame(gdf)
        if overture_type == "segment":
            walking_types = ['motorway', 'primary','secondary', 'tertiary']
            gdf = gdf[gdf['class'].isin(walking_types)]
            
            # Filter out rows with null geometries BEFORE estimating UTM
            # gdf = gdf[~gdf.geometry.isna()]
            
            # Now you can safely estimate UTM and buffer
            if gdf is not None and not gdf.empty:
                gdf['geometry'] = gdf.to_crs(gdf.estimate_utm_crs()).buffer(25).to_crs('EPSG:4326')
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

# Filter by voltage

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
