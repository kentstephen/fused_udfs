@fused.udf
def udf(path: str='s3://fused-users/stephenkentdata/brooklyn_parks_iso.gejosn'):
    import geopandas as gpd

    extension = path.rsplit(".", maxsplit=1)[-1]
    driver = (
        "GPKG"
        if extension == "gpkg"
        else ("ESRI Shapefile" if extension == "shp" else "GeoJSON")
    )
    gdf = gpd.read_file(path, driver=driver)
    print(gdf)
    # if preview:
    #     return gdf.geometry
    print(gdf.columns)
    print(gdf.head())
    return gdf
