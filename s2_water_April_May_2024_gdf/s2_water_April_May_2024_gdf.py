@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int = 13,
       time_of_interest="2024-04-01/2024-05-01"):
    import geopandas as gpd
    import shapely
    from utils import add_rgb
    import numpy as np
    
    bounds = bbox.bounds.values[0]
    x, y, z = bbox.iloc[0][["x", "y", "z"]]
    print(x, y,z )
    res_offset = -1
    resolution = max(min(int(3 + bbox.z[0] / 1.5), 12) - res_offset, 2)
    
    print(resolution)
    s2_udf = fused.load('https://github.com/fusedio/udfs/tree/7e47784/public/S2_explorer/')

    ds = fused.run(udf=s2_udf,time_of_interest=time_of_interest,provider="AWS", x=x, y=y, z=z)
    print(ds)
    # # This worked:
    arr = ds.image.sel(band=2).values
    
    # So let's structure water mask the same way:
    water_mask = (ds.image.sel(band=2) < 30).values.astype(float) #NIR
    # arr = dataset.image.sel(band=3).values # Shortwave infared
    # df = dataset.image.sel(band=1).to_dataframe('value').reset_index()
    # print(df)
                       
    # # arr = dataset.to_array().values
    # arr = np.stack([dataset['B08'].values, dataset['B11'].values, dataset['B03-B04'].values])
    arr_to_latlng = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_latlng
    df_water = arr_to_latlng(water_mask, bounds)
    print(df_water)
    # print(df_pop)
    def df_to_hex(df, resolution, latlng_cols=("lat", "lng")):
        con = fused.utils.common.duckdb_connect()
        query = f"""
                SELECT h3_latlng_to_cell(lat, lng, {resolution}) AS hex, 
                h3_cell_to_boundary_wkt(hex) boundary,
                sum(data) as data
                FROM df
                group by 1
              --  order by 1
            """
        df = con.sql(query).df()
        gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
        return gdf
        
    gdf = df_to_hex(df_water, resolution)
    gdf = gdf[gdf["data"] > 0]
    # print(df["data"].describe())
    # df = add_rgb(df, 'data')
    print(gdf)
    return gdf
    # nir = ds.image.sel(band=2).values
    # print("NIR min:", nir.min())
    # print("NIR max:", nir.max())
    # print("\nSome sample values:")
    # print(nir.flatten()[:100])  # Look at first 20 values
