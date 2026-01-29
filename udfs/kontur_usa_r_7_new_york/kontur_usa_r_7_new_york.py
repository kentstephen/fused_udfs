@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    import pandas as pd
    from utils import get_kontur, get_cells, add_rgb
    tract = gpd.read_file("https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/36_NEW_YORK/36/tl_2020_36_tract20.zip")
    if tract.crs != "EPSG:4326":
        tract = tract.to_crs("EPSG:4326")
    tract = tract.dissolve()
    # Convert the geometry column to WKT strings while maintaining the column name
    tract['geometry'] = tract['geometry'].apply(shapely.wkt.dumps)
    
    # Convert to regular pandas DataFrame
    df_tract = pd.DataFrame(tract)
    df_kontur = get_kontur()
    # print(df_kontur.columns)
    # print(df_kontur.head())
    # bounds = bbox.bounds.values[0]
    df = get_cells(df_tract, df_kontur)
    print(df)
    # df = add_rgb(df, 'pop')
    return df