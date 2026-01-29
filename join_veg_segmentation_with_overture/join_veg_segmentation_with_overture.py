@fused.udf 
def udf(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-09-18-0",
    theme: str = None,
    overture_type: str = None, 
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None,
    resolution: int = 11
    
):
    # print(bbox)
    from utils import get_overture
    import geopandas as gpd
    import geopandas as gpd
    import numpy as np
    import pandas as pd
    import shapely
    import xarray
    from shapely.geometry import Polygon
    veg_udf = fused.load("stephen.kent.data@gmail.com/Vegetation_Segmentation")
    arr = fused.run(udf=veg_udf, bbox=bbox, return_mask=True)
    # print(type(arr))
    print("Dataset dimensions:", arr.dims)
    print("Dataset coordinates:", arr.coords)
    # print(arr)
    import numpy as np
    import geopandas as gpd
    from shapely.geometry import Point
    
    def xarray_to_point_gdf(ds, x, y, z):
        # Function to convert tile coordinates to lat/lon
        def tile_to_latlon(x, y, z):
            n = 2.0 ** z
            lon_deg = x / n * 360.0 - 180.0
            lat_rad = np.arctan(np.sinh(np.pi * (1 - 2 * y / n)))
            lat_deg = np.degrees(lat_rad)
            return lat_deg, lon_deg
    
        # Calculate the top-left corner of the tile
        lat_top, lon_left = tile_to_latlon(x, y, z)
        
        # Calculate the bottom-right corner of the tile
        lat_bottom, lon_right = tile_to_latlon(x + 1, y + 1, z)
        
        # Get dimensions from the dataset
        y_dim, x_dim = ds.dims['y'], ds.dims['x']
        
        # Calculate pixel size in degrees
        pixel_size_lat = (lat_top - lat_bottom) / y_dim
        pixel_size_lon = (lon_right - lon_left) / x_dim
    
        # Create empty list to store our geometries
        geometries = []
        
        # Get the data array (assuming it's named 'image')
        arr = ds['image'].values
        
        # Determine which band to use for point creation
        if arr.ndim == 3 and arr.shape[0] == 4:  # It's likely a mask with RGBA channels
            band_to_check = 1  # Green channel for vegetation mask
        elif arr.ndim == 3:
            band_to_check = 0  # First band for multi-band image
        else:
            band_to_check = None  # For 2D arrays
        
        # Iterate through the data
        for y_idx in range(y_dim):
            for x_idx in range(x_dim):
                value = arr[band_to_check, y_idx, x_idx] if band_to_check is not None else arr[y_idx, x_idx]
                if value > 0:  # Assuming 0 is the background/no data value
                    # Calculate lat/lon for this pixel
                    lat = lat_top - (y_idx + 0.5) * pixel_size_lat
                    lon = lon_left + (x_idx + 0.5) * pixel_size_lon
                    
                    # Create a point geometry
                    geometries.append(Point(lon, lat))
        
        # Create a GeoDataFrame with just the geometries
        gdf = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:4326")  # Assuming WGS84
        
        return gdf
    
    # Create the GeoDataFrame
    gdf = xarray_to_point_gdf(arr, bbox.x[0], bbox.y[0], bbox.z[0])
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    # print(gdf.columns)
    df_veg = pd.DataFrame(gdf)
    duckdb_connect = fused.load(
            "https://github.com/fusedio/udfs/tree/a1c01c6/public/common/"
        ).utils.duckdb_connect
    con = duckdb_connect()
    def read_data(resolution, df_veg):
        df = con.sql("""
        SELECT h3_h3_to_string(h3_latlng_to_cell(ST_Y(ST_GeomFromText(geometry)), ST_X(ST_GeomFromText(geometry)), $resolution)) as cell_id,
               h3_cell_to_boundary_wkt(cell_id) as boundary,
               count(1) as cnt
        FROM df_veg
        GROUP BY cell_id
       
        """, params={'resolution': resolution}).df()
        return df

    df = read_data(resolution, df_veg)
    gdf_veg = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))

    gdf_overture = get_overture(
        bbox=bbox,
        release=release,
        theme=theme,
        overture_type=overture_type,
        use_columns=use_columns,
        num_parts=num_parts,
        min_zoom=min_zoom,
        polygon=polygon,
        point_convert=point_convert
    )
    gdf_joined = gpd.sjoin(gdf_overture, gdf_veg, how="inner", predicate="intersects")
    print(gdf_joined['cnt'].describe())
    return gdf_joined