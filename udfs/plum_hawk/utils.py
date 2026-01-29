import duckdb
def get_con():
    
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
                LOAD h3ext;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_region='us-west-2';""")
    return con

import geopandas as gpd
from shapely.geometry import box
import pandas as pd

def get_overture(bbox):
    # Create a shapely box from the bbox coordinates
    polygon = box(*bbox)
    
    # Create a GeoDataFrame with geometry as Shapely objects
    gdf = gpd.GeoDataFrame(index=[0], crs='EPSG:4326', geometry=[polygon])
    
    # Load the UDF and run it with the bbox GeoDataFrame
    udf = fused.load("stephen.kent.data@gmail.com/Overture_Maps_Example")
    overture = fused.run(udf=udf, bbox=gdf)
    
    # Convert the geometry column to WKT
    overture['geometry'] = overture['geometry'].apply(lambda geom: geom.wkt if geom else None)
    
    # Convert the resulting GeoDataFrame to a regular DataFrame if needed
    df = pd.DataFrame(overture)
    
    return df
import pandas as pd
from shapely import wkt
import geopandas as gpd
import pynhd
from shapely import box
buffer_distance= 100
@fused.cache
def get_water(bbox):
    

    
    # Fetch flowlines and water bodies
    wd_flowlines = pynhd.WaterData("nhdflowline_network")
    flw_gdf = wd_flowlines.bybox(bbox)
    
    wd_waterbodies = pynhd.WaterData("nhdwaterbody")
    water_bodies_gdf = wd_waterbodies.bybox(bbox)
    
    # Combine flowlines and water bodies into one GeoDataFrame
    combined_gdf = pd.concat([flw_gdf, water_bodies_gdf], ignore_index=True)
    
    # Convert WKT strings to Shapely geometries if needed and set the CRS
    combined_gdf['geometry'] = combined_gdf['geometry'].apply(lambda geom: wkt.loads(geom) if isinstance(geom, str) else geom)
    combined_gdf = gpd.GeoDataFrame(combined_gdf, geometry='geometry')
    
    # Check and set CRS if not already set
    if combined_gdf.crs is None:
        combined_gdf.set_crs(epsg=4326, inplace=True)
    
    # Convert to EPSG:3857 for buffering
    combined_gdf = combined_gdf.to_crs(epsg=3857)
    
    # Apply the buffer
    combined_gdf['geometry'] = combined_gdf['geometry'].buffer(buffer_distance)
    
    # Convert back to EPSG:4326
    combined_gdf = combined_gdf.to_crs(4326)
    
    # Convert GeoDataFrame back to DataFrame with geometries as WKT
    result_df = pd.DataFrame(combined_gdf)
    result_df['geometry'] = result_df['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    return result_df

buildings_query = """    
    CREATE OR REPLACE TABLE buildings AS
WITH latlang AS (
    SELECT       
        ST_Y(ST_Centroid(ST_GeomFromText(geometry))) AS latitude,
        ST_X(ST_Centroid(ST_GeomFromText(geometry))) AS longitude
    FROM buildings_df
), to_cells AS (
    SELECT
        h3_latlng_to_cell(latitude, longitude, $resolution) AS cell_id,
        COUNT(1) AS cnt
    FROM latlang
    GROUP BY 1
) 
SELECT
    h3_h3_to_string(cell_id) AS cell_id,
    SUM(cnt) AS cnt
FROM to_cells
GROUP BY 1;

    """ 
final_query ="""
      WITH w_buffer AS (
        SELECT
            ST_GeomFromText(geometry) AS water_buffer
        FROM water_df
    )
    SELECT
    
        b.cell_id AS cell_id,
       -- h3_cell_to_boundary_wkt(h3_string_to_h3(b.cell_id)) AS boundary,
        b.cnt
    FROM buildings b
    JOIN w_buffer w
    ON ST_Intersects(ST_GeomFromText(h3_cell_to_boundary_wkt(b.cell_id)), w.water_buffer)
    GROUP BY ALL;
    """ 