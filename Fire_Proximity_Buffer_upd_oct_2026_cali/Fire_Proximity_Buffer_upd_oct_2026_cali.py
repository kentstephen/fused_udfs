@fused.udf
def udf(
    bounds: fused.types.Bounds=[-117.644, 34.416, -117.639, 34.421],
    date_start: int = 2,
    max_records: int = 100,
):
    import geopandas as gpd
    import shapely
    gdf_fire = load_fire_buffer_gdf(max_records)
    # gdf = gpd.GeoDataFrame(gdf_fire)
    # gdf_fire['geometry'] = gdf_fire['geometry'].apply(shapely.wkb.dumps)
    print(gdf_fire)
    return gdf_fire

# @fused.cache
def load_fire_buffer_gdf(max_records: int):
    import requests
    import geopandas as gpd
    import datetime
    import pandas as pd
    from math import isnan
    
    # Function to convert unix to datetime
    def date_string(timestamp: int | float):
        if not timestamp or isnan(timestamp):
            return None
        return datetime.datetime.utcfromtimestamp(timestamp / 1000).strftime("%Y-%m-%d")
    
    # Fetch the GeoJSON data from the remote URL
    @fused.cache
    def load_data(max_records):
        # Source: CAL FIRE Historical Fire Perimeters
        base_url = "https://services1.arcgis.com/jUJYIo9tSA7EHvfZ/arcgis/rest/services/California_Fire_Perimeters/FeatureServer/0/query"
        
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "resultRecordCount": max_records,
            "orderByFields": "YEAR_ DESC"
        }
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        gdf_fire = gpd.GeoDataFrame.from_features(data["features"])
        return gdf_fire
    gdfs = []
    gdf_fire = load_data(max_records)
    # return gdf_fire
    cols = ["FIRE_NAME", "ALARM_DATE", "CONT_DATE", "YEAR_", "GIS_ACRES", "AGENCY", "CAUSE", "geometry"]
    gdf_fire = gdf_fire[cols]
    gdf_fire["_ALARM_DATE"] = gdf_fire["ALARM_DATE"].apply(date_string)
    gdf_fire["_CONT_DATE"] = gdf_fire["CONT_DATE"].apply(date_string)
    gdf_fire.crs = "EPSG:4326"
    # gdf = (pd.concat(gdfs))
    return gdf_fire
    buffers = {
        "within_historic_perimeter": {"distance": 0, "score": 3},
        "near_historic_perimeter": {"distance": 1_000, "score": 2},
        "outside_perimeter": {"distance": 10_000, "score": 1},
    }
    gdfs = []
    for buffer_name, buffer in buffers.items():
        _gdf = gdf_fire.copy()
        _gdf = _gdf.to_crs(_gdf.estimate_utm_crs())
        _gdf["geometry"] = _gdf["geometry"].buffer(buffer["distance"])
        _gdf["score"] = buffer["score"]
        _gdf = _gdf.to_crs("EPSG:4326")
        _gdf["buffer_name"] = buffer_name
        gdfs.append(_gdf)
    return pd.concat(gdfs)