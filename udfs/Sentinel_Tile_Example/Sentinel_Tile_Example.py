@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    provider="AWS",
    time_of_interest="2020-01-01/2023-12-30",
    h3_size: int=10
):  
    """
    This UDF returns Sentinel 2 NDVI of the passed bounding box (viewport if in Workbench, or {x}/{y}/{z} in HTTP endpoint)
    Data fetched from either AWS S3 or Microsoft Planterary Computer
    """
    import odc.stac
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    import utils
    import xarray as xr
    
    if provider == "AWS":
        # AWS Sentinel-2 band names
        red_band = "red"
        nir_band = "nir"
        swir1_band = "swir16"  # SWIR 1.6µm
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif provider == "MSPC":
        # Microsoft Planetary Computer Sentinel-2 band names
        red_band = "B04"
        nir_band = "B08"
        swir1_band = "B11"  # SWIR 1.6µm
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
    else:
        raise ValueError(
            f'{provider=} does not exist. provider options are "AWS" and "MSPC"'
        )
    
    items = catalog.search(
        collections=["sentinel-2-l2a"],
        bbox=bbox.total_bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": 10}},
    ).item_collection()
    
    # Capping resolution to min 10m, the native Sentinel 2 pixel size
    resolution = int(10 * 2 ** max(0, (15 - bbox.z[0])))
    print(f"{resolution=}")

    if len(items) < 1:
        print(f'No items found. Please either zoom out or move to a different area')
    else:
        print(f"Returned {len(items)} Items")
    
    ds = odc.stac.load(
            items,
            crs="EPSG:4326",
            bands=[swir1_band, nir_band],
            resolution=resolution,
            bbox=bbox.total_bounds,
        ).astype(float)

    first_image = ds.isel(time=0)
    last_image = ds.isel(time=-1)
    
    # Calculate NDBI for both timepoints (SWIR1 - NIR)/(SWIR1 + NIR)
    first_ndbi = (first_image[swir1_band] - first_image[nir_band]) / (first_image[swir1_band] + first_image[nir_band])
    last_ndbi = (last_image[swir1_band] - last_image[nir_band]) / (last_image[swir1_band] + last_image[nir_band])
    
    # Calculate change in built-up index
    ndbi_change = last_ndbi - first_ndbi
    
    # Filter for positive changes (new construction)
    new_construction = xr.where(ndbi_change > 0.01, ndbi_change, 0)  # Threshold can be adjusted
    
    # Get values array
    arr = new_construction.values
        
        # rgb_image = utils.visualize(
        #     data=arr,
        #     min=0,
        #     max=1,
        #     colormap=['black', 'green']
        # )
        # return rgb_image
    bounds = bbox.bounds.values[0]
    arr_to_latlng = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_latlng
    
    df_vv = arr_to_latlng(arr, bounds)

    print(df_vv['data'].describe())
    print(df_vv)
    # print(boundary)
    def df_to_hex(df_vv,bounds, h3_size):
        
        
        xmin, ymin, xmax, ymax = bounds
        con = fused.utils.common.duckdb_connect()
        qr = f"""
with to_grid as(
SELECT 
    h3_latlng_to_cell(lat::double, lng::double, {h3_size}::integer) AS hex,
    unnest(h3_grid_disk(hex, 3)) as disk_hex,
    data
FROM df_vv
)
select 
    disk_hex as hex,
    h3_cell_to_lat(disk_hex) cell_lat,
    h3_cell_to_lng(disk_hex) cell_lng,
    avg(data) as data
from to_grid
WHERE (cell_lat >= {ymin}
    AND cell_lat < {ymax}
    AND cell_lng >= {xmin}
    AND cell_lng < {xmax})
group by 1, 2, 3

        """
        return con.query(qr).df()
    
    df = df_to_hex(df_vv,bounds, h3_size)
    df = df[df["data"] >0]
    print(df['data'].describe())
    return df

