@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int=11,
     red_band="green",
    nir_band="swir16",
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
        red_band=red_band,
        nir_band=nir_band,
        time_of_interest=time_of_interest,
        bbox=bbox)
    print(ds)
    # arr_to_latlng = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_latlng
   
    # Basic dataset info
    print(ds.image)
    
    # Print band information specifically
    print("Band information:")
    for band in ds.band.values:
        print(f"Band {band}")

# Alternative detailed info
    print(ds.info())
    # Try to access band descriptions/wavelength info
    print("\nBand descriptions:")
    print(ds.band.description if hasattr(ds.band, 'description') else "No description available")
    
    # Check all attributes
    print("\nAll dataset attributes:")
    print(ds.attrs)
    
    # Try accessing band-specific metadata
    print("\nImage variable attributes:")
    print(ds.image.attrs)
    # print("Loaded bands:", ds.band_names) # or ds.bands if available
# # Assuming band 4 is Red and band 3 is NIR for your data:
#     ndvi = (ds.image.sel(band=4) - ds.image.sel(band=3)) / (ds.image.sel(band=4) + ds.image.sel(band=3)) # Since band dimension is 1, index 0 selects it
#     arr_to_latlng = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_latlng
#     df_ndvi = arr_to_latlng(ndvi.values, bounds)
#     # print(ndbi.values)
#     print(df_ndvi["data"].describe())
#     print(df_ndvi)
#     # print(df_ndvi)
#     def df_to_hex(df_ndvi, resolution):
#         utils = fused.load(
#             "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
#         ).utils
#         qr = f"""
#                 SELECT h3_latlng_to_cell(lat, lng, {resolution}) AS hex, avg(data) as data
#                 FROM df_ndvi
                
#                 group by 1
#               --  order by 1
#             """
#         con = utils.duckdb_connect()
#         return con.query(qr).df()
        
#     df = df_to_hex(df_ndvi, resolution)
#     # df = df[df["data"] > 0]
#     # df = add_rgb(df, 'data')
#     # df["data"] = (df["data"] - df["data"].min()) / (df["data"].max() - df["data"].min())
#     # print(df)
#     print(df["data"].describe())

#     print(df)
#     return df