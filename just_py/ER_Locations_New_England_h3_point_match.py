@fused.udf
def udf(bbox=None,resolution: int = 8):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import get_mask
    @fused.cache
    def get_hospitals():
        bbox = gpd.GeoDataFrame(
        geometry=[shapely.box(-73.63,42.64,-66.89,47.58)], # new england
        crs=4326
        )
        gdf = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
        return gdf[gdf['fsq_category_ids']=='4bf58dd8d48988d194941735']
        # mask = get_mask()
        # return gpd.overlay(gdf, mask, how='intersection')
        
    gdf = get_hospitals()
    def get_cells(gdf):
        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.dumps)
        df = pd.DataFrame(gdf)
        con = fused.utils.common.duckdb_connect()
        query=f"""
    SELECT h3_h3_to_string(h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution})) AS hex,
             string_agg(DISTINCT name, ', ') as name
             from df
             group by 1
"""
        return con.sql(query).df()
    df = get_cells(gdf)
    return df