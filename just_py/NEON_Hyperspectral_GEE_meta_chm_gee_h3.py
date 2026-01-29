# Note: Place your GEE credentials json in the `key_path` and set your `acct_serv`

@fused.udf#(cache_max_age="0s")
def udf(
    bounds: fused.types.Bounds=None,
    acct_serv: str ='fused-nyt-gee@fused-nyt.iam.gserviceaccount.com',
      res:int=12,
    res_factor:int=10,
    tree_scale: int = 30,
    show_dem: bool= True
):
    import duckdb
    import pandas as pd
    import ee
    import xarray
    import numpy as np
    import xee
    import json
    from utils import aggregate_df_hex
    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = utils.get_tiles(bounds)
    zoom = tile.iloc[0].z

    # Authenticate GEE
    key_path = '/mount/gee_key.json'
    credentials = ee.ServiceAccountCredentials(acct_serv, key_path)
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials)
    bounds_gdf = utils.bounds_to_gdf(bounds)
    bounds_values = bounds_gdf.bounds.values[0]
    # Create collection
    # geom = ee.Geometry.Rectangle(*bounds)
    # geom = ee.Geometry.Rectangle(*bounds_gdf.total_bounds)
    # scale = 1 / 2 ** max(0, zoom-2)  # Get 4x larger tiles
    # # scale = 30
    # Use exact tile boundaries from Fused
    tile_bounds = tile.iloc[0].geometry.bounds
    geom = ee.Geometry.Rectangle(*tile_bounds)  # Use exact web tile bounds
    
    # Use a scale that aligns with web tiles
    scale = 1 / 2 ** max(0, zoom)  # Standard web tile scaling
    # ic = ee.ImageCollection("projects/sat-io/open-datasets/facebook/meta-canopy-height")
    # scale = 1 / 2 ** max(0, zoom)

    # Create geometry and load data
    # geom = ee.Geometry.Rectangle(*bounds)
    # Use the ETH dataset that was mentioned in the GEE app
    # Filter the collection to only tiles that intersect this specific tile's bounds
    ic = ee.ImageCollection("projects/sat-io/open-datasets/facebook/meta-canopy-height")\
        .filterBounds(geom)
        # .limit(1)  # Get just the first/best tile for this area
    
    # Now open with xarray - should have actual data instead of NaN
   # Instead of .isel(time=0), use max across time to blend tiles
    # Instead of .isel(time=0), use max across time to blend tiles
    # ds = xarray.open_dataset(ic, engine='ee', geometry=geom, scale=scale).max(dim='time')
    # arr = ds['cover_code'].values.astype('uint8')
    # Add buffer to geometry to get overlapping data
    buffer_size = 1000  # Large buffer in meters
    geom_buffered = ee.Geometry.Rectangle(*bounds_gdf.total_bounds).buffer(buffer_size)
    ds = xarray.open_dataset(ic, engine='ee', geometry=geom_buffered, scale=scale).isel(time=0)
    
    # Then crop back to original bounds after processing
    arr = ds['cover_code'].values.astype('uint8')
    # ds = xarray.open_dataset(ic, engine='ee', geometry=geom, scale=scale).isel(time=0)
    # ds = xarray.open_dataset(ic, engine='ee', geometry=geom, scale=scale).isel(time=0)
    # arr = ds['cover_code']
    print("Dataset info:")
    print(ds)
    
    # # Check what variables are actually available
    # print("\nAvailable variables:", list(ds.data_vars.keys()))
    
    # # Check the cover_code variable specifically
    # print("\ncover_code info:")
    print(ds['cover_code'])
    
    # # Check data range
    # cover_data = ds['cover_code']
    # print(f"\nData stats:")
    # print(f"Min: {cover_data.min().values}")
    # print(f"Max: {cover_data.max().values}")
    # print(f"Mean: {cover_data.mean().values}")
    # print(f"Shape: {cover_data.shape}")
    
    # # Check for any valid (non-zero, non-null) values
    # valid_count = (cover_data != 0).sum().values
    # total_count = cover_data.size
    # print(f"\nValid (non-zero) pixels: {valid_count} out of {total_count}")
    
    # # Sample a few actual values to see what's there
    # print(f"\nFirst 5x5 corner of data:")
    # print(cover_data.values[:5, :5])
    
    # # Check if bounds are reasonable
    # print(f"\nBounds used: {bounds}")
    # print(f"Scale used: {scale}")
    # # Check if the geometry is being created correctly
    # print(f"Original bounds: {bounds}")
    # print(f"GEE geometry: {geom.getInfo()}")
    
    # # Check the tile info
    # print(f"Tile info:")
    # print(tile)
    # print(f"Zoom level: {zoom}")
    # print(f"Scale: {scale}")
    
    # # Try loading without .isel(time=0) first
    # ds_full = xarray.open_dataset(ic, engine='ee', geometry=geom, scale=scale)
    # print(f"Full dataset before time selection:")
    # print(ds_full)
    
    # # Check if the collection has data in your area
    # # Add this before opening with xarray:
    # collection_info = ic.getInfo()
    # print(f"Collection info: {collection_info}")
    # R = ds['R'].values.squeeze().T.astype(float)
    # G = ds['G'].values.squeeze().T.astype(float)
    # B = ds['B'].values.squeeze().T.astype(float)
    # A = B.copy()
    
    # R[R<1]=np.nan
    # G[G<1]=np.nan
    # B[B<1]=np.nan
    # A[B==np.nan]=0
    # # Get RGB values and stack them
    # arr = np.stack([R,G,B,A], axis =0)
    
    # arr_scaled = np.clip(arr, 0, 255).astype(np.uint8)
    # df = utils.arr_to_latlng(arr, bounds)
    # lons, lats = np.meshgrid(arr.x.values, arr.y.values)
    # lons, lats = np.meshgrid(arr.lon.values, arr.lat.values)
    # df = pd.DataFrame({
    #     'lng': lons.flatten(),
    #     'lat': lats.flatten(), 
    #     'data': arr.values.flatten()
    # })
    df = utils.arr_to_latlng(arr, bounds)
    df = df.dropna() 
    bounds_gdf = utils.bounds_to_gdf(bounds)
    bounds_values = bounds_gdf.bounds.values[0]
    df = aggregate_df_hex(df=df,stats_type='mean', res=res, bounds_values=bounds_values)
    df["elev_scale"] = int((15 - zoom) * 1)
    import numpy as np
    df['viz_metric'] = np.where(df['canopy_height'] == 0, 0, np.sqrt(df['canopy_height']) * tree_scale)
    def water(tile, res):
        df = fused.run("fsh_6lHjnX9dqsbiLnXwPE8BRC", bounds=tile, res=res)
        if df is None or df.empty:  # Changed from df_water to df
            return None
        return df
    
    df_water = water(tile, res)
    if df_water is not None:
        # print(df_water)
        if show_dem is True:
            df_dem = fused.run("fsh_3M3RyItkeAZpGR6fMZ482r", bounds=tile, res=res) # jaxa
            # df_dem = fused.run("fsh_2KKOTd6HSiGtNOYHLqG4xN", bounds=tile, res=res) # USGS dem_10meter_tile_hex_2
            df = duckdb.sql("""
        select df.hex, df.canopy_height,
               df_dem.metric + df.viz_metric as total_elevation 
        from df 
        inner join df_dem on df.hex = df_dem.hex
         where df.hex not in (
          select hex from df_water where hex is not null
       )
            """).df()
            # df["elev_scale"] = int((15 - zoom) * 1)
            # return df
            print(df)
    else: 
            df_dem = fused.run("fsh_3M3RyItkeAZpGR6fMZ482r", bounds=tile, res=res) # jaxa
            # df_dem = fused.run("fsh_2KKOTd6HSiGtNOYHLqG4xN", bounds=tile, res=res) # USGS dem_10meter_tile_hex_2
            df = duckdb.sql("""
        select df.hex, df.canopy_height,
               df_dem.metric+ df.viz_metric as total_elevation 
        from df 
        inner join df_dem on df.hex = df_dem.hex
       --  where df.hex not in (
         -- select hex from df_water where hex is not null
       --)
            """).df()
             
    
    df["elev_scale"] = int((15 - zoom) * 1)
    

    # df = df[df['metric']>0]
    # print(df)
    # df_dem = fused.run("fsh_3ailz7Y3EAoBnz7G4pPYPJ", bounds=bounds, res=res, tree_scale=tree_scale, show_dem=show_dem) # meta trees
    # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) # hexify
    # df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=tile, h3_size=res) #hexify
    # df_dem = fused.run("fsh_2KKOTd6HSiGtNOYHLqG4xN", bounds=tile, res=res) # USGS dem_10meter_tile_hex_2
    # df = duckdb.sql(" select df.hex, coalesce(df.metric,5.01) as ndvi, df_dem.total_elevation, df_dem.canopy_height from df left join df_dem on df.hex = df_dem.hex-- where canopy_height >0").df()
    # df_roads = get_over(tile, overture_type="land_use")
    # bounds = utils.bounds_to_gdf(bounds)
    # bounds = bounds.bounds.values[0]
    # df = run_query(df_roads, df_sentinel, res, bounds)
    # df['elevation'] = df["elevation"] - 2000
    print(df)
    # df['ndvi'] = df['ndvi']-5
    return df
    # rgb_image = c
  