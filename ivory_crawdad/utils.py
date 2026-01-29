import shapely
import pandas as pd
import geopandas as gpd

def get_fsq(bounds):
        gdf = fused.run("UDF_Foursquare_Open_Source_Places", bounds=bounds, min_zoom=0)
        gdf['lat'] = gdf.geometry.centroid.y
        gdf['lng'] = gdf.geometry.centroid.x

        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)
def get_overture(bounds, overture_type):
        overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
        gdf = overture_utils.get_overture(bbox=bounds, overture_type=overture_type, min_zoom=0)
        if gdf is None or gdf.empty:
            return
 
        gdf['lat'] = gdf.geometry.centroid.y
        gdf['lng'] = gdf.geometry.centroid.x
            # gdf = gdf.drop(columns=['geometry'])
      
        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        return pd.DataFrame(gdf)
def latlng_to_cell(df, resolution):
        # Calculate H3 cells for both datasets
        utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
        con = utils.duckdb_connect()
        
        # Add H3 cells to Overture
        query = f"""
        SELECT *, h3_latlng_to_cell(lat, lng, {resolution}) as hex
        FROM df
        """
        return con.sql(query).df()
