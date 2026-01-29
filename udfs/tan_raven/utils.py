import pandas as pd
import shapely 
import geopandas as gpd
def get_over(bounds, overture_type):
        overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
        gdf = overture_utils.get_overture(bbox=bounds, overture_type=overture_type, min_zoom=0)
        if gdf is None or gdf.empty:
            return
        if overture_type=="place":
            gdf['lat'] = gdf.geometry.centroid.y
            gdf['lng'] = gdf.geometry.centroid.x
            gdf = gdf.drop(columns=['geometry'])
        elif overture_type=='land_use':
            gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)
@fused.cache(reset=True)
def run_query(df_buildings, df_dem, res, bounds):

    xmin, ymin, xmax, ymax = bounds
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Connect to DuckDB
    con = utils.duckdb_connect()
    query=f"""with buildings_to_cells as (
    select 
        unnest(h3_polygon_wkt_to_cells(geometry, {res})) as hex,
        id,
        subtype
    from df_buildings
) 

select
    b.id, 
    b.hex,
  --  coalesce(avg(b.height), 1) as height,
    --coalesce(avg(b.height), 1) + avg(d.metric) as elevation,
    d.metric as elevation,
    b.subtype
 
from buildings_to_cells b inner join df_dem d
on b.hex=d.hex
where
    h3_cell_to_lat(b.hex) >= {ymin}
    AND h3_cell_to_lat(b.hex) < {ymax}
    AND h3_cell_to_lng(b.hex) >= {xmin}
    AND h3_cell_to_lng(b.hex) < {xmax}
group by all
    """
    return con.sql(query).df()
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