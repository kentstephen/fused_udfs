@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=7):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb
    @fused.cache
    def get_fsq_points():
        bbox = gpd.GeoDataFrame(
        geometry=[shapely.box(-79.76,40.48,-71.79,45.02)], # nyc
        crs=4326
    )

        return fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
        
    df_poi = get_fsq_points()

    # print(f"df_poi type{type(df_poi)}")
    # Load the boroughs GeoJSON file
    # boroughs_gdf = gpd.read_file("https://data.cityofnewyork.us/api/geospatial/tqmj-j8zm?method=export&format=GeoJSON", driver="GeoJSON")

    # Load the boroughs GeoJSON file
    tract = gpd.read_file("https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/36_NEW_YORK/36/tl_2020_36_tract20.zip")
    if tract.crs != "EPSG:4326":
        tract = tract.to_crs("EPSG:4326")
    
    # Perform an overlay between df_poi and the dissolved geometry
    df_poi = gpd.overlay(df_poi, tract, how='intersection')
    df_poi['geometry'] = df_poi['geometry'].apply(shapely.wkt.dumps)
    df_poi = pd.DataFrame(df_poi)
    if df_poi is None or df_poi.empty:
         return pd.DataFrame()
    df_kontur = fused.run("fsh_2vLUXhJ8XYbpuqx3ff9pS3")
    # # Filter for rows where 'level3_category_name' contains "Coffee Shop"
    # df_coffee fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox)
    # df_coffee = df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
    
    # # Exclude rows where 'name' contains "Starbucks"
    # df_coffee = df_coffee[~df_coffee["name"].str.contains("Starbucks", case=False, na=False)]
    # if df_coffee is None or df_coffee.empty:
    #     return pd.DataFrame()
    
    @fused.cache
    def get_cells(df_poi, df_kontur, resolution):
        con = fused.utils.common.duckdb_connect()
        query=f"""
WITH place_cnt AS (
    SELECT
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), {resolution}) AS hex,
        COUNT(1) AS poi_cnt
    FROM df_poi
    GROUP BY 1
)
SELECT
    h3_h3_to_string(k.hex) AS hex,
    COALESCE(p.poi_cnt, 0) AS poi_cnt,
    SUM(pop) AS pop,
    CASE
        WHEN k.pop = 0 THEN NULL
        ELSE COALESCE(p.poi_cnt, 0) / k.pop
    END AS place_ratio
FROM df_kontur k
LEFT JOIN place_cnt p ON k.hex = p.hex
group by k.hex

        

        
        """
        return con.sql(query).df()

    df = get_cells(df_poi, df_kontur, resolution)
    print(df)
    # df = add_rgb(df, 'poi_cnt')
    return df