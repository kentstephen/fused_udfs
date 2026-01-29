@fused.udf
def udf(bounds: fused.types.Tile= None, target_resolution: int =11):
    import geopandas as gpd
    import shapely
    import fused
    import geopandas as gpd
    import shapely
    
    bounds = gpd.GeoDataFrame(
        geometry=[shapely.box(-74.9278,40.287,-72.8734,41.2009)], 
        crs=4326
    )

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
  final_hex AS hex,
  avg(pop) AS pop  -- Sum where cells overlap
FROM expanded_grid
GROUP BY 1
HAVING pop >= 5;
        """
        return con.sql(query).df()
    df = run_query(df=df_kontur, target_resolution=target_resolution)
    print(df)
    return df