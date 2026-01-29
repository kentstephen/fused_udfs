@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int=12,
        red_band="swir16",
    nir_band="nir08",
        time_of_interest="2021-04-01/2022-06-30"
       ):
    print(f"z: {bbox.z}")
    from utils import add_rgb
    import geopandas as gpd
    import shapely
    res_offset = -1  # lower makes the hex finer
    # resolution = max(min(int(3 + bbox.z[0] / 1.5), 12) - res_offset, 2)
    bounds = bbox.bounds.values[0]
    ds = fused.run(
        "UDF_Landsat_Tile_Example", 
        time_of_interest=time_of_interest,
        red_band=red_band,
    nir_band=nir_band,
        bbox=bbox)
    arr_to_latlng = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_latlng
    # print(dataset)
   # Print basic information about the dataset
    # print("Dataset info:")
    # print(ds)
    
    # # Print the values for each band to inspect
    # print("\nBand 1 (likely Red):")
    # print(ds['image'].sel(band=1).values)
    
    # print("\nBand 2 (likely NIR):")
    # print(ds['image'].sel(band=2).values)
    
    # print("\nBand 3 (likely SWIR1):")
    # print(ds['image'].sel(band=3).values)
    
    # print("\nBand 4 (likely SWIR2):")
    # print(ds['image'].sel(band=4).values)
    # ndbi = (ds['image'].sel(band=3) - ds['image'].sel(band=2)) / (ds['image'].sel(band=3) + ds['image'].sel(band=2))



# Assuming band 4 is Red and band 3 is NIR for your data:
    ndvi = (ds.image.sel(band=4) - ds.image.sel(band=3)) / (ds.image.sel(band=4) + ds.image.sel(band=3)) # Since band dimension is 1, index 0 selects it
    arr_to_latlng = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_latlng
    df_ndvi = arr_to_latlng(ndvi.values, bounds)
    # print(ndbi.values)
    print(df_ndvi["data"].describe())
    print(df_ndvi)
    # print(df_ndvi)
    def df_to_hex(df_ndvi, resolution):
        utils = fused.load(
            "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
        ).utils
        qr = f"""With to_cells as(
                SELECT h3_latlng_to_cell(lat, lng, {resolution}) AS hex, avg(data) as data
                FROM df_ndvi
                 group by 1
                --where data < 0.5
                 ), aggregated_ndvi AS (
    SELECT 
        h3_cell_to_parent(hex, 8) AS parent_cell,
        AVG(data) AS avg_ndvi,
        COUNT(*) AS cell_count
    FROM to_cells
    GROUP BY parent_cell
)
SELECT
    parent_cell as hex,
    avg_ndvi AS data
FROM aggregated_ndvi


               
              --  order by 1
            """
        con = utils.duckdb_connect()
        return con.query(qr).df()
        
    df = df_to_hex(df_ndvi, resolution)
    # df = df[df["data"] > 0]
    # df = add_rgb(df, 'data')
    # df["data"] = (df["data"] - df["data"].min()) / (df["data"].max() - df["data"].min())
    # print(df)
    print(df["data"].describe())

    print(df)
    return df