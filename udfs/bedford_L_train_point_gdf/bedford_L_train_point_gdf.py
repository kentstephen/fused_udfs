@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    geojson="""
    {
  "type": "Feature",
  "properties": {
    "fsq_category_ids": "4bf58dd8d48988d1fd931735",
    "name": "MTA Subway - Bedford Ave (L)",
    "level1_category_name": "Travel and Transportation",
    "level2_category_name": "Transport Hub",
    "level3_category_name": "Metro Station",
    "__index_level_0__": 469
  },
  "geometry": {
    "type": "Point",
    "coordinates": [
      -73.95769509630115,
      40.71771330421802
    ]
  }
}"""
    gdf = gpd.read_file(geojson)
    return gdf