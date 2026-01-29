import pandas as pd
import shapely 
import geopandas as gpd
def get_over(tile, overture_type):
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    gdf = fused.utils.Overture_Maps_Example.get_overture(bounds=tile, overture_type=overture_type, min_zoom=0)
    if gdf is None or gdf.empty:
        return pd.DataFrame({})
    
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
def run_query(df_roads, df_sentinel, res, bounds):

    xmin, ymin, xmax, ymax = bounds
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""--with buildings_to_cells as (
    --select 
        -- unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'center')) as hex,
      --  id,
    --    height
  --  from df_buildings
--),
with roads_to_cells as (
select
 unnest(h3_polygon_wkt_to_cells_experimental(geometry, {res}, 'overlap')) as hex,
 subtype,
 class
 from df_roads
) 

SELECT
  s.hex,
  r.subtype,
  r.class,
-- s.agg_band1,
 -- s.agg_band2,
--  s.agg_band3,
 -- s.metric
FROM 
  df_sentinel s 

 LEFT JOIN 
  roads_to_cells r ON s.hex = r.hex

where
    h3_cell_to_lat(s.hex) >= {ymin}
    AND h3_cell_to_lat(s.hex) < {ymax}
    AND h3_cell_to_lng(s.hex) >= {xmin}
    AND h3_cell_to_lng(s.hex) < {xmax}
--group by all
    """
    return con.sql(query).df()
def add_rgb_cmap(gdf, key_field, cmap_dict):
    import pandas as pd
    def get_rgb(value):
        if pd.isna(value):
            print(f"Warning: NaN value found in {key_field}")
            return [128, 128, 128]  # Default color for NaN values
        if value not in cmap_dict:
            print(f"Warning: No color found for {value}")
        return cmap_dict.get(value, [128, 128, 128])  # Default to gray if not in cmap

    rgb_series = gdf[key_field].apply(get_rgb)
    
    gdf['r'] = rgb_series.apply(lambda x: x[0])
    gdf['g'] = rgb_series.apply(lambda x: x[1])
    gdf['b'] = rgb_series.apply(lambda x: x[2])
    
    
    
    return gdf
CMAP = {
    "agriculture": [255, 223, 186],  # Light orange
    "aquaculture": [173, 216, 230],  # Light blue
    "campground": [205, 133, 63],  # Brown
    "cemetery": [192, 192, 192],  # Silver
    "construction": [255, 165, 0],  # Orange
    "developed": [169, 169, 169],  # Dark gray
    "education": [144, 238, 144],  # Light green
    "entertainment": [255, 10, 80],  # Hot pink
    "golf": [34, 139, 34],  # Forest green
    "grass": [124, 252, 0],  # Lawn green
    "horticulture": [255, 222, 173],  # Light goldenrod
    "landfill": [139, 69, 19],  # Saddle brown
    "managed": [220, 220, 220],  # Gainsboro
    "medical": [255, 99, 71],  # Tomato red
    "military": [0, 128, 128],  # Teal
    "park": [60, 179, 113],  # Medium sea green
    "pedestrian": [250, 128, 114],  # Salmon
    "protected": [85, 107, 47],  # Dark olive green
    "recreation": [100, 149, 237],  # Cornflower blue
    "religious": [153, 50, 204],  # Dark orchid
    "residential": [255, 255, 0],  # Yellow
    "resource_extraction": [139, 0, 0],  # Dark red
    "transportation": [70, 130, 180],  # Steel blue
    "winter_sports": [176, 224, 230],  # Powder blue
}
