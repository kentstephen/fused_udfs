@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection="3dep-seamless",
    band="data",
    res_factor:int=1,
    res:int= 10
):

    import odc.stac
    import palettable
    import planetary_computer
    import pystac_client
    import pandas as pd
    from shapely.geometry import Point
    import geopandas as gpd
    from pystac.extensions.eo import EOExtension as eo
    from utils import aggregate_df_hex

    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    items = catalog.search( 
        collections=[collection],
        bbox=bounds,
    ).item_collection()
    print(items[0].assets.keys()) 
    print(f"Returned {len(items)} Items")
    resolution = int(20/res_factor * 2 ** (max(0, 13 - zoom)))
    print(f"{resolution=}")
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[band],
        resolution=resolution,
        bbox=bounds,
    ).astype(float)
    
    # Use data from the most recent time.
    arr = ds[band].max(dim="time")
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    bounds = utils.bounds_to_gdf(bounds)
    bounds = bounds.bounds.values[0]
    df = utils.arr_to_latlng(arr.values, bounds)
    df['geometry'] = df.apply(lambda row: Point(row['lng'], row['lat']), axis=1)

# Convert the pandas DataFrame to a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    
    # Set the coordinate reference system (CRS)
    # WGS84 is a common geographic coordinate system
    gdf.set_crs(epsg=4326, inplace=True)
    return gdf
        # df = df_to_hex(df, res)
    # df = aggregate_df_hex(df=df,stats_type='mean', res=res)
    # # df =df[df['metric']<4857]
    # print(df)
    # return df
    # # Visualize that data as an RGB image.
    # After converting array to dataframe
    # After converting array to dataframe (before hexagonal binning)
    # print(f"Raw dataframe shape: {df.shape}")
    # print(f"Raw dataframe data range: {df['data'].min()} to {df['data'].max()}")
    # print(f"Raw dataframe zero count: {(df['data'] == 0).sum()}")
    
    # # # After hexagonal binning
    # df_hex = aggregate_df_hex(df=df, stats_type='mean', res=res)
    # print(f"Hex dataframe shape: {df_hex.shape}")
    # print(f"Hex dataframe metric range: {df_hex['metric'].min()} to {df_hex['metric'].max()}")
    # print(f"Hex dataframe zero count: {(df_hex['metric'] == 0).sum()}")
    # return df_hex
    # rgb_image = utils.visualize(
    #     data=arr,
    #     min=0,
    #     max=100,
    #     colormap=palettable.matplotlib.Viridis_20,
    # )
    # return rgb_image