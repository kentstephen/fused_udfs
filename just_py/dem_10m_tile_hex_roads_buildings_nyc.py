# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection="3dep-seamless",
    band="data",
    res_factor:int=4,
    res:int=12
):

    import odc.stac
    
    import palettable
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    from utils import aggregate_df_hex, df_to_hex

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
    if not items or len(items) == 0:
        print("No items found for the given bounds and collection")
        return
    # print(items[0].assets.keys()) 
    # print(f"Returned {len(items)} Items")
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
    if arr is None:
        return
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    # bounds = utils.bounds_to_gdf(bounds)
    # bounds_values = bounds.bounds.values[0]
    df = utils.arr_to_latlng(arr.values, bounds)
    
    df = aggregate_df_hex(df=df,stats_type='mean', res=res)
    @fused.cache
    def buildings(df, bounds, res):
        df_building = fused.run("fsh_3o1ev6ebOHwIC20CPC28Sz", bounds=bounds, res=res)
        if df_building is not None and not df_building.empty:
            # Get unique hex values from df_building
            building_hex_values = df_building['hex'].unique()
            
            # Create a dictionary mapping hex values to heights
            hex_to_height = dict(zip(df_building['hex'], df_building['height']))
            
            # Apply heights and update metric for matching hex values
            for hex_value in building_hex_values:
                # Create mask for matching hex values
                mask = df['hex'] == hex_value
                
                # Add height column for matching rows
                df.loc[mask, 'height'] = hex_to_height.get(hex_value, 0)
                
                # Add 5 to the metric value for all matching rows
                df.loc[mask, 'metric'] += 5
                
                # Calculate total_elevation by adding metric and height
                df.loc[mask, 'metric'] = df.loc[mask, 'metric'] + df.loc[mask, 'height']
        
        # Return df outside the if block to ensure it's always returned
        return df
        
    # Call the function
    # df = buildings(df, bounds, res)
    @fused.cache
    def roads(df, bounds, res):
        df_road = fused.run("fsh_6N6n5fHUn7U1g2MLsM0w45", bounds=bounds, res=res)
        if df_road is not None and not df_road.empty:
            road_hex_values = df_road['hex'].unique()
                # road_hex_values = set(df_building['hex'].unique())
        
        # Create a boolean mask for all matching hex values at once
            mask = df['hex'].isin(road_hex_values)
            road_hex_values = set(df_road['hex'].unique())
        
        # Create a boolean mask for all matching hex values at once
            mask = df['hex'].isin(road_hex_values)
        
        # Add 5 to the metric value for all matching rows in one operation
            df.loc[mask, 'metric'] = df.loc[mask, 'metric'] + 5
        return df
    # df['metric'] = df["metric"] - 1000
    # print(df)
    # df = roads(df, bounds, res)
    print(df.columns)
    return df

    # rgb_image = utils.visualize(
    #     data=arr,
    #     min=0,
    #     max=100,
    #     colormap=palettable.matplotlib.Viridis_20,
    # )
    # return rgb_image