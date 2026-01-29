@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=7):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import add_rgb
    res_offset = 0  # lower makes the hex finer
    # resolution = max(min(int(3 + bbox.z[0] / 1.5), 12) - res_offset, 2)
    print(resolution)
    df_places = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    df_places = df_places[df_places['level3_category_name'].str.contains('vegan', case=False, na=False)]
    df_places['geometry'] = df_places['geometry'].apply(shapely.wkt.dumps)
    if df_places is None or df_places.empty:
        return pd.DataFrame()
    con = fused.utils.common.duckdb_connect()
    def get_cells(resolution, df_places):
        query= """
        select
        h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), 9) as hex,
        count(1) as cnt
        from df_places
        --where level1_category_name  = 'Dining and Drinking'
        group by 1
        order by cnt desc
        """
        df = con.sql(query).df()
        return df
    
    df = get_cells(resolution, df_places)
    df = add_rgb(df, 'cnt')
    return df
    
    