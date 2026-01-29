@fused.udf
def udf(
    bounds: fused.types.Bounds=None,
    var: str = 'transition', 
    # var: str = 'max_extent', # 
    return_object: str='hex', # TODO: whether to return H3 or a polygon of the H3
    res_offset: int = -1,  # lower makes the hex finer
    res: int= 11
): 
    import ee
    import xarray
    import numpy as np
    import pandas as pd
    import geopandas as gpd
    import json
    import h3
    from utils import color_map, description_mapping,get_data,get_summary,rgb_to_hex
    utils = fused.load('https://github.com/fusedio/udfs/tree/a18669/public/common/').utils
    bounds_gdf = utils.bounds_to_gdf(bounds)
    z = utils.estimate_zoom(bounds)
    bounds_gdf['z'] = z

    if 'z' not in bounds_gdf.columns:
        bounds_gdf['z'] = 10

    # Set your own creds
    utils.ee_initialize(service_account_name='fused-nyt-gee@fused-nyt.iam.gserviceaccount.com',key_path="/mount/gee_key.json")
    geom = ee.Geometry.Rectangle(*bounds_gdf.total_bounds)

    # Surface water
    ic_gsw = ee.ImageCollection(ee.Image('JRC/GSW1_4/GlobalSurfaceWater'))
    ds = xarray.open_dataset(ic_gsw, engine='ee', geometry=geom,scale=1/2**max(0,z) )
    ds_gsw = xarray.open_dataset(
        ic_gsw,
        engine='ee',
        geometry=geom,
        scale=1/2**max(0,z) 
    ).max(dim='time')
    print('vars', ds_gsw)
    arr = ds_gsw[var].values.astype('uint8').T
    
    # Hexify
    # res = max(min(int(3 + z / 1.5), 12) - res_offset, 2)
    df = utils.arr_to_h3(arr, bounds_gdf.total_bounds, res=res, ordered=False)

    # find most frequet land_type
    df['most_freq'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[0][np.argmax(np.unique(x, return_counts=True)[1])])
    df['n_pixel'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[1].max())
    df=df[df['most_freq']>0]
    if len(df)==0: return 

    # get the color and land_type
    df[['r', 'g', 'b', 'a']] = [0, 0, 255, 255]
    df['transition_type'] = df.most_freq.map(description_mapping)
    df=df[['hex', 'transition_type', 'r', 'g', 'b', 'a', 'most_freq','n_pixel']]
    # df['hex'] = df['hex'].apply(lambda x: h3.int_to_str(x))
    # df_dem = fused.run("fsh_3M3RyItkeAZpGR6fMZ482r", bounds=bounds, res=res) # jaxa
    df_dem = fused.run("fsh_5FZlHaVg8BqL2Eh2KTWevp", bounds=bounds, h3_size=res) # hexify
    import duckdb
    df = duckdb.sql("select df.*, df_dem.metric from df inner join df_dem on df.hex = df_dem.hex").df()
    print(df)
    return df

    if return_object == 'hex':
        # print(df)
        return df

    from shapely.geometry import Polygon
    import h3
    df['geometry'] = df['hex'].astype(str).apply(lambda x: Polygon([(lon, lat) for lat, lon in h3.cell_to_boundary(x)]))
    gdf = gpd.GeoDataFrame(df)
    return gdf



    