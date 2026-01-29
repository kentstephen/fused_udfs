@fused.udf
def udf(bounds: fused.types.Tile= None, target_resolution: int =11):
    import geopandas as gpd
    import shapely
    import fused
    import geopandas as gpd
    import shapely
    
    bounds = gpd.GeoDataFrame(
        geometry=[shapely.box(-74.258843,40.476578,-73.700233,40.91763)], 
        crs=4326
    )
    # df_buildings = fused.run("fsh_7j8xMh7ybV9upNnpOe7eNW", bounds=bounds, resolution=target_resolution)
    df_kontur = fused.run("fsh_1Kg290kb0BPNahbrby4Kah", bounds=bounds)
    # bounds = bounds.bounds.values[0]
    def run_query(df, target_resolution):
        # xmin, ymin, xmax, ymax = bounds
        utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
        # Connect to DuckDB
        con = utils.duckdb_connect()
        query=f"""
PRAGMA max_temp_directory_size='10GiB';

-- Current resolution is always 8 (Kontur data)
-- Only {target_resolution} needs to be specified

-- First increase resolution dynamically
WITH higher_resolution AS (
  SELECT 
    unnest(h3_cell_to_children(hex, {target_resolution})) AS hex,
    -- Dynamically calculate division factor based on resolution difference from 8
    -- Each resolution level increase multiplies cells by 7
   pop
  FROM df
),
-- Then apply grid_disk at the higher resolution
expanded_grid AS (
  SELECT 
    unnest(h3_grid_disk(hex, 7)) AS final_hex,
    -- Divide by 7 for immediate neighbors in grid_disk with radius 1
    pop
  FROM higher_resolution
)
SELECT
 e.final_hex AS hex,
 h3_cell_to_boundary_wkt(hex) as boundary,
 avg(e.pop) AS pop  -- Sum where cells overlap
FROM expanded_grid E
GROUP BY 1
HAVING pop >= 5;
        """
        df = con.sql(query).df()
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    gdf = run_query(df=df_kontur, target_resolution=target_resolution)
    # return gdf
    # print(df)x
    # return df
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    gdf_overture= overture_utils.get_overture(bbox=bounds, min_zoom=0)
    gdf_joined = gdf_overture.sjoin(gdf, how="inner", predicate="intersects")

    gdf_joined = gdf_joined.drop(columns='index_right')
    return gdf_joined