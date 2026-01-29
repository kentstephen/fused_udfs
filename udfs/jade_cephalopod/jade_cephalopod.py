@fused.udf
def udf(bbox=None, n: int=100000):
    import geopandas as gpd
    import shapely
    from utils import get_con, get_points, add_rgb_to_df
    resolution = 8

    # return gpd.GeoDataFrame({"value": list(range(n))}, geometry=geoms)
    rand_df = get_points(n=n)
    con = get_con()
    query ="""
    SELECT
            h3_latlng_to_cell(latitude, longitude, $resolution) AS cell_id,
            COUNT(1) AS cnt
        FROM rand_df
        GROUP BY cell_id
    """
    df = con.sql(query, params={'resolution': resolution}).df()
    df = add_rgb_to_df(df, 'cnt')
    print(df)
    return df