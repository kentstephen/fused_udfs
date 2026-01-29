@fused.udf
def udf(bounds: fused.types.Tile = None, 
        resolution: int = 12):
    from utils import get_overture, run_query, add_rgb

    # Retrieve a DataFrame of segment data from the bounding box (bbox)
    df_segment = get_overture(bounds)

    # Establish a connection to DuckDB
    
    df_dem = fused.run("fsh_4bYjNAhhOKALqYMJAP4Sxh", bounds=bounds, h3_size=resolution)
    # Run a query that converts the LineStrings in df_segment into H3 cells
    df = run_query(resolution, df_segment, df_dem)

    # Add RGB values to the DataFrame using the 'total_value' column, 
    # fitting the colors to Matplotlib colormaps
    df = add_rgb(df, 'total_value')

    print(df)
    return df
