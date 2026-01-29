@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int = 11):
    import geopandas as gpd
    import shapely
    from utils import add_rgb
    import numpy as np
    
    bounds = bbox.bounds.values[0]
    x, y, z = bbox.iloc[0][["x", "y", "z"]]
    print(x, y,z )
    res_offset = 0
    # h3_size = max(min(int(3 + bbox.z[0] / 1.5), 12) - res_offset, 2)
    
    # print(resolution)
    s2_udf = fused.load('https://github.com/fusedio/udfs/tree/7e47784/public/S2_explorer/')

    dataset = fused.run(udf=s2_udf,time_of_interest="2021-01-01/2021-11-13",provider="AWS", x=x, y=y, z=z)
    print(dataset)
    arr = dataset.image.sel(band=2).values #NIR
    # arr = dataset.image.sel(band=3).values # Shortwave infared
    # df = dataset.image.sel(band=1).to_dataframe('value').reset_index()
    # print(df)
                       
    # # arr = dataset.to_array().values
    # arr = np.stack([dataset['B08'].values, dataset['B11'].values, dataset['B03-B04'].values])
    arr_to_h3 = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_h3
    df = arr_to_h3(arr=arr, bounds=bounds, res=resolution, ordered=False)
    print(df)
    # df = df[df["data"] > 0]
    # print(df["data"].describe())
    # df = add_rgb(df, 'data')
    # print(df)
    # return df
