@fused.udf
def udf(bbox, context, year="2022", water_buffer:float = 100):
    from utils import (
        get_lulc,
        get_overture,
        arr_to_color,
        LULC_IO_COLORS,
        get_con,
        get_arrow
    )
    import numpy as np
    import rasterio
    import rasterio.features
    import shapely
    import geopandas as gpd
    import pyarrow as pa
    from shapely import wkt
    import pandas as pd
    
    print(f"Zoom level: {bbox.z[0]}")
    if bbox.z[0] >= 10:
        if bbox.z[0] > 15:
            return None
        data = get_lulc(bbox, year)
        
        transform = rasterio.transform.from_bounds(*bbox.total_bounds, 256, 256)
        shapes = rasterio.features.shapes(data, data == 1, transform=transform)
        geoms = []
        for shape, shape_value in shapes:
            shape_geom = shapely.geometry.shape(shape)
            geoms.append(shape_geom)  
            
        water_gdf = gpd.GeoDataFrame({}, geometry=[shapely.geometry.MultiPolygon(geoms)],
                                     crs=bbox.crs)
        
        if len(geoms) and water_buffer:
            water_utm = water_gdf.estimate_utm_crs()
            print(f"Estimated UTM CRS: {water_utm}")
            water_gdf = water_gdf.to_crs(water_utm)
            water_gdf.geometry = water_gdf.geometry.buffer(water_buffer)
            water_gdf = water_gdf.to_crs(bbox.crs)
            
        gdf = get_overture(water_gdf)
        print(gdf.crs)
        # gdf = gdf.to_crs(4326)
        
        con = get_con()
        resolution = max(min(int(6 + (bbox.z[0] - 10) * (5/9)), 11), 0)
        building_table = get_arrow(gdf)
      
        # building_table = pa.Table.from_pandas(buildings_df)
        buildings_query = """    
        WITH latlang AS (
            SELECT       
                ST_Y(ST_Centroid(ST_GeomFromText(geometry))) AS latitude,
                ST_X(ST_Centroid(ST_GeomFromText(geometry))) AS longitude
            FROM building_table
        ), to_cells as(
            SELECT
                h3_h3_to_string(h3_latlng_to_cell(latitude, longitude, $resolution)) AS cell_id,
                COUNT(1) AS cnt
            FROM latlang
            GROUP BY 1
        )
        SELECT
            cell_id,
            SUM(cnt) as cnt
        FROM to_cells
        GROUP BY cell_id
        """ 
        h3_df = con.sql(buildings_query, params={'resolution': resolution}).df()

        return h3_df
        print(h3_df)
    elif bbox.z[0] > 14:
        del h3_df
        return None
        
    else:
        print("Zoom in more..")
        return None