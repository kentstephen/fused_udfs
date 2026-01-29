import pandas as pd
from shapely import wkt
import geopandas as gpd
import pynhd
from shapely import box

def get_water(bbox, buffer_distance):
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

import mercantile



def bbox_to_xyz(bbox):
    west, south, east, north = bbox
    
    # Find the center of the bounding box
    center_lon = (west + east) / 2
    center_lat = (south + north) / 2
    
    # Get the tile containing the center point
    # mercantile.tile automatically chooses an appropriate zoom level
    tile = mercantile.tile(center_lon, center_lat)
    
    return tile.x, tile.y, tile.z
import geopandas as gpd
import pandas as pd
from shapely import wkt, wkb

def gdf_to_pandas_with_wkt(gdf):
    # Create a copy of the GeoDataFrame
    df = gdf.copy()
    
    # First, ensure geometries are in WKB format and load them
    df['geometry'] = df['geometry'].apply(lambda geom: wkb.loads(geom.wkb) if geom is not None else None)
    
    # Convert the GeoDataFrame to a regular pandas DataFrame
    df = pd.DataFrame(df)
    
    # Now convert the geometry to WKT format
    df['geometry'] = df['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    
    # Finally, apply WKT loads to ensure all geometries are in WKT format
    df['geometry'] = df['geometry'].apply(lambda wkt_str: wkt.loads(wkt_str) if wkt_str is not None else None)
    
    return df
import pandas as pd
import concurrent.futures
import logging
from shapely import wkb, wkt
from shapely.geometry import box

def get_overture(bbox):
    table_path = "s3://us-west-2.opendata.source.coop/fused/overture/2024-03-12-alpha-0/theme=buildings/type=building/part=*/*.parquet"
    use_columns = ['geometry']  # Add any other columns you need

    def process_chunk(chunk_bbox):
        try:
            # Read the Parquet file with bbox filter
            df = pd.read_parquet(table_path, columns=use_columns, filters=[('geometry', chunk_bbox)])
            
            if df.empty:
                return None
            
            # Process the geometry column
            df['geometry'] = df['geometry'].apply(wkb.loads)
            df['wkb'] = df['geometry'].apply(wkb.dumps)
            df['wkt'] = df['geometry'].apply(wkt.dumps)
            
            return df[['wkb', 'wkt']]
        except Exception as e:
            logging.warning(f"Failed to process chunk: {str(e)}")
            return None

    # Split the bbox into smaller chunks
    bbox_geom = box(*bbox)
    num_chunks = 4  # Adjust this based on your needs
    chunk_width = bbox_geom.bounds[2] - bbox_geom.bounds[0]
    chunk_height = bbox_geom.bounds[3] - bbox_geom.bounds[1]
    
    chunks = []
    for i in range(num_chunks):
        for j in range(num_chunks):
            minx = bbox_geom.bounds[0] + i * (chunk_width / num_chunks)
            miny = bbox_geom.bounds[1] + j * (chunk_height / num_chunks)
            maxx = bbox_geom.bounds[0] + (i + 1) * (chunk_width / num_chunks)
            maxy = bbox_geom.bounds[1] + (j + 1) * (chunk_height / num_chunks)
            chunks.append((minx, miny, maxx, maxy))

    # Process chunks concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_chunks*num_chunks) as executor:
        results = list(executor.map(process_chunk, chunks))

    # Combine results
    combined_df = pd.concat([df for df in results if df is not None], ignore_index=True)
    
    if combined_df.empty:
        logging.warning("No data found in the given bbox")
        return None

    return combined_df