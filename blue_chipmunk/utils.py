import pyarrow.parquet as pq
import geopandas as gpd
import mercantile
from shapely.geometry import box

def table_to_tile(table, bounds):
    """Tiler for cloud-optimized GeoParquet
    
    Parameters:
    - file_path: Path to cloud-optimized GeoParquet
    - x, y, z: Tile coordinates
    """
    common = fused.load("https://github.com/fusedio/udfs/tree/435a367/public/common/").utils
    # Get tile bounds
    tile = common.get_tiles(bounds)
    print(tile)
    # tile = common.to_gdf(tile)
    tile_values = gpd.GeoDataFrame(
            {
                "geometry": [
                    box(
                        bounds[0],
                        bounds[1],
                        bounds[2],
                        bounds[3],
                    )
                ]
            }
        )
    tile_bbox = [bounds[0], bounds[1], bounds[2], bounds[3]]
    path = table.replace('s3://', '')
    df = gpd.read_parquet(f"s3://{path}")
    # tile = tile.dissolve().reset_index(drop=True)
    df.crs = tile.crs
    print(df)
    df = df[df.intersects(tile.geometry[0])]
    print(df)
    # Convert to GeoDataFrame
    # gdf = gpd.GeoDataFrame(df)
    
  
    return df