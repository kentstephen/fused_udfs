# todo: investigate why sometime configure_s3_access get cached
@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    time_of_interest="2005-08-30/2005-09-10",
    green_band="green",
    swir_band="swir16",
    collection="landsat-c2-l2",
    cloud_threshold=10,
    resolution = 11
):
    """Display NDVI based on Landsat & STAC"""
    import odc.stac
    import shapely
    import palettable
    import pystac_client
    import numpy as np
    from pystac.extensions.eo import EOExtension as eo
    import geopandas as gpd
    # Load utility functions.
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/5cfb808/public/common/"
    ).utils

    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    
    # Search for imagery within a specified bounding box and time period
    items = catalog.search(
        collections=[collection],
        bbox=bbox.total_bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": cloud_threshold}},
    ).item_collection()
    print(f"Returned {len(items)} Items")

    # Determine the pixel spacing for the zoom level
    pixel_spacing = int(5 * 2 ** (15 - bbox.z[0]))
    print(f"{pixel_spacing = }")

    # Load imagery into an XArray dataset
    odc.stac.configure_s3_access(requester_pays=True)
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[swir_band, green_band],
        resolution=pixel_spacing,
        bbox=bbox.total_bounds,
    ).astype(float)
    print(ds)
    # After loading the dataset, let's print some statistical information
    print("\nGreen band statistics:")
    print(f"Green min: {ds[green_band].min().values}")
    print(f"Green max: {ds[green_band].max().values}")
    print(f"Green mean: {ds[green_band].mean().values}")
    
    print("\nSWIR band statistics:")
    print(f"SWIR min: {ds[swir_band].min().values}")
    print(f"SWIR max: {ds[swir_band].max().values}")
    print(f"SWIR mean: {ds[swir_band].mean().values}")
    
    # print(ds.image)
    
    # Print band information specifically
    # print("Band information:")
    # for band in ds.band.values:
        # print(f"Band {band}")
     # Normalize the bands to 0-1 range first
    # green_normalized = ds[green_band] / 10000.0
    # swir_normalized = ds[swir_band] / 10000.0
    # Calculate MNDWI
    # mndwi = (green_normalized - swir_normalized) / (green_normalized + swir_normalized)
    # Using the same structure but with green and SWIR bands for MNDWI
    # Since your bands are already separate as 'green' and 'swir16'
    # Calculate MNDWI
    mndwi = (ds['green'] - ds['swir16']) / (ds['green'] + ds['swir16'])
    
    # Take first time slice to remove time dimension
    # mndwi = mndwi.isel(time=0)  # This selects the first time slice
    
    bounds = bbox.bounds.values[0]
    arr_to_latlng = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_latlng
    df_mndwi = arr_to_latlng(mndwi.values, bounds)
    # print(ndbi.values)
    print(df_mndwi["data"].describe())
    print(df_mndwi)
    # print(df_ndvi)
    def df_to_hex(df_mndwi, resolution):
        con = fused.utils.common.duckdb_connect()
        qr = f"""
WITH agg_data AS (
    SELECT 
        lat,
        lng,
        AVG(data) as data
    FROM df_mndwi
    GROUP BY lat, lng
    -- Lower threshold to filter out ocean/permanent water bodies
    HAVING AVG(data) < 0.08
)
SELECT 
    h3_latlng_to_cell(lat, lng, {resolution}) AS hex,
    h3_cell_to_boundary_wkt(hex) as boundary,
    AVG(data) as data
FROM agg_data
WHERE data > 0.0
GROUP BY hex;
              --  order by 1
            """

        df = con.query(qr).df()
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
        
    df = df_to_hex(df_mndwi, resolution)
    # df = df[df["data"] > 0]
    # df = add_rgb(df, 'data')
    # df["data"] = (df["data"] - df["data"].min()) / (df["data"].max() - df["data"].min())
    # print(df)
    print(df["data"].describe())

    print(df)
    # return df
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, use_columns=["height"], min_zoom=10)
    
    # Join H3 with Buildings using coffe_score to visualize, you can change to a left join
    gdf_joined = gdf_overture.sjoin(df, how="inner", predicate="intersects")

    gdf_joined = gdf_joined.drop(columns='index_right')
    return gdf_joined