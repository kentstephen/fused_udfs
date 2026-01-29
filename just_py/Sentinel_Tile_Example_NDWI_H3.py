def udf(
    bbox: fused.types.TileGDF=None,
    provider="MSPC",
    time_of_interest="2025-01-01/2025-01-10",
    resolution: int = 11
    
):  
    """
    This UDF returns water bodies using NDWI from Sentinel 2 imagery
    """
    import odc.stac
    import planetary_computer
    import pystac_client
    import utils
    bounds = bbox.bounds.values[0]
    if provider == "AWS":
        green_band = "green"
        nir_band = "nir"
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif provider == "MSPC":
        green_band = "B03"
        nir_band = "B08"
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
    else:
        raise ValueError(f'{provider=} does not exist. provider options are "AWS" and "MSPC"')
    
    items = catalog.search(
        collections=["sentinel-2-l2a"],
        bbox=bbox.total_bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": 10}},
    ).item_collection()
    
    # resolution = 10
    # print(f"{resolution=}")
    
    if len(items) < 1:
        print(f'No items found. Please either zoom out or move to a different area')
    else:
        print(f"Returned {len(items)} Items")
        
        ds = odc.stac.load(
            items,
            crs="EPSG:3857",
            bands=[green_band, nir_band],
            resolution=resolution,
            bbox=bbox.total_bounds,
        ).astype(float)
        
        # Calculate NDWI
        ndwi = (ds[green_band] - ds[nir_band]) / (ds[green_band] + ds[nir_band])
        arr = ndwi.max(dim="time")
        arr_to_latlng = fused.load("https://github.com/fusedio/udfs/tree/7204a3c/public/common/").utils.arr_to_latlng
        df_ndwi = arr_to_latlng(arr.values, bounds)
    # print(ndbi.values)
        print(df_ndwi["data"].describe())
        print(df_ndwi)
        # print(df_ndvi)
        def df_to_hex(df_ndwi, resolution):
            con = fused.utils.common.duckdb_connect()
            qr = f"""
  
    SELECT 
        h3_latlng_to_cell(lat, lng, {resolution}) AS hex,
        avg(data) as data
    FROM df_ndwi
    where data > 0.1
    GROUP BY hex
    --having avg(data) > 0
                  --  order by 1
                """
    
            return con.query(qr).df()
        
        # rgb_image = utils.visualize(
        #     data=arr,
        #     min=-0.0,
        #     max=0.1,
        #     colormap=['white', 'blue']
        # )
    df = df_to_hex(df_ndwi, resolution)
    print(df)
    return df