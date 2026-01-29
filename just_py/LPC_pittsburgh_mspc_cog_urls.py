@fused.udf(cache_max_age="0s")
def udf(bounds: fused.types.Bounds= [-80.095517, 40.36152, -79.865728, 40.501202], collection="3dep-lidar-copc"):
    import pandas as pd
    import planetary_computer
    import pystac_client
    
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    
    items = catalog.search(collections=[collection], bbox=bounds).item_collection()
    
    df = pd.DataFrame([
        {
            'id': item.id,
            'url': item.assets['data'].href,
            'points': item.properties['pc:count'],
            'min_x': item.properties['proj:bbox'][0],
            'min_y': item.properties['proj:bbox'][1],
            'max_x': item.properties['proj:bbox'][3],
            'max_y': item.properties['proj:bbox'][4],
        }
        for item in items
    ])
    print(df)
    return df