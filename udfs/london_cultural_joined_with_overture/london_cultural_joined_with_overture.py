@fused.udf
def udf(bbox: fused.types.TileGDF=None,
    release: str = "2024-09-18-0",
    theme: str = "buildings", 
    overture_type: str = "building",
    use_columns: list = ["geometry"],
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None
):
    import geopandas as gpd
    import shapely
    from utils import get_data, get_con, get_overture, add_rgb_cmap, CMAP
    culture_df = get_data()
    print(culture_df)
   
   
    con = get_con()
    print(con)
    # print(con)
    resolution = 10
    
    query = f"""    

        WITH h3_cte AS (
            SELECT
                type AS name,
                h3_latlng_to_cell(latitude, longitude, {resolution}) AS cell_id
            FROM culture_df
            
        ),
        name_counts AS (
            SELECT
                cell_id,
                name,
                COUNT(*) AS name_count
            FROM h3_cte
            GROUP BY cell_id, name
        ),
        most_frequent_names AS (
            SELECT
                cell_id,
                name,
                name_count AS cnt
            FROM name_counts
            QUALIFY ROW_NUMBER() OVER (PARTITION BY cell_id ORDER BY name_count DESC) = 1
        )
        SELECT 
            h3_h3_to_string(cell_id) AS cell_id,
            h3_cell_to_boundary_wkt(cell_id) as boundary,
            name AS type,
            cnt
        FROM most_frequent_names;
    
        """
    df = con.sql(query).df()
    gdf_culture = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    
    overture_udf = fused.load('https://github.com/fusedio/udfs/tree/c8c3c40/public/Overture_Maps_Example/')
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

    if gdf_overture is not None and not gdf_overture.empty:
        gdf_joined = gdf_overture.sjoin(gdf_culture)
        # gdf_joined = add_rgb(gdf_joined, 'cnt') 
        print(gdf_joined)
        gdf_joined = add_rgb_cmap(gdf=gdf_joined, key_field="type", cmap_dict=CMAP)
        return gdf_joined
    else:
        # Return an empty GeoDataFrame with the same structure as gdf_overture
        gdf_joined = gpd.GeoDataFrame(columns=gdf_overture.columns if gdf_overture is not None else [])
        print("No data in gdf_overture")