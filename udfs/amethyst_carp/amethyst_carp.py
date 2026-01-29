@fused.udf
def udf(bbox: fused.types.TileGDF=None):
    import geopandas as gpd
    import shapely
    from shapely import wkt
    import pandas as pd
    from utils import get_con
    # @fused.cache
    # def get_df():
        # Read the parquet file into a GeoDataFrame
    gdf = gpd.read_parquet("s3://fused-users/stephenkentdata/fused-tmp/NYC_NSI.parquet")
    
    # Convert the geometry to WKT (Well-Known Text) format
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom else None)
    
    # Convert the GeoDataFrame to a regular pandas DataFrame
    nsi_df = pd.DataFrame(gdf)
    
    
        
    # trying to recreate the DC AOI Hex Equation
    # h3_size = max(min(int(15 - (bbox.z[0] - 3)), 15), 0)
    h3_size = 7
    

    con = get_con()
    query = f""" with geo as(
                    SELECT  
                        ST_Centroid(ST_GeomFromText(geometry)) AS geometry,
                        stats
                FROM nsi_df 
                ) 
          
                SELECT 
                    h3_h3_to_string(h3_latlng_to_cell(ST_Y(geometry), ST_X(geometry), {h3_size})) AS cell_id,
                    SUM(stats) as stats
                FROM geo
                GROUP BY cell_id
    
            """
    df = con.sql(query).df
    print(df)
    return df