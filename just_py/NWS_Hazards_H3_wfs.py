@fused.udf
def udf(bbox: fused.types.Bbox=None, crs="EPSG:4326", res=7):
    import fused
    import geopandas as gpd
    import h3
    import json
    from owslib.wfs import WebFeatureService
    from utils import CMAP, add_rgb_cmap

    # Watch/Warning/Advisory (WWA) Web Feature Service url
    url = 'https://mapservices.weather.noaa.gov/eventdriven/services/WWA/watch_warn_adv/MapServer/WFSServer'
    type_name = 'watch_warn_adv:WatchesWarnings'
    max_features = 1000
    start_index = 0
    all_features = []

    # Connect to WFS
    try:
        wfs = WebFeatureService(url=url, version='2.0.0')
    except Exception as e:
        print(f"Failed to connect to WFS: {e}")
        return None

    # Fetch data with pagination
    while True:
        try:
            response = wfs.getfeature(
                typename=type_name,
                startindex=start_index,
                maxfeatures=max_features,
                outputFormat='GEOJSON',
            )
            data = response.read().decode('utf-8')
            features = json.loads(data).get("features", [])
            
            if not features:
                break

            all_features.extend(features)
            start_index += max_features

            if len(features) < max_features:
                break

        except Exception as e:
            print(f"Error fetching data from WFS: {e}")
            break

    if not all_features:
        print("No features fetched from WFS.")
        return None

    # Generate GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(all_features, crs=crs)
    print(len(gdf))
    return gdf