# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, buffer_multiple: float = 1):
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/7206323/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    import pystac
    import rasterio
    import xarray as xr
    import rioxarray
    
    # Example workflow (you'll need to find the specific STAC catalog URL)
    # The dataset is distributed over 22,063 tiles according to the paper
    
    # Option 1: Direct COG access if you know the tile structure
    # You can construct URLs based on the tile naming convention
    
    # Option 2: Through STAC if there's a catalog
    # catalog = pystac.Catalog.from_file("catalog_url")
    # collection = catalog.get_collection("ctrees-amazon-canopy-height")
    
    # Option 3: Direct rasterio access to COGs
    cog_url = "s3://bucket/path/to/tile.tif"  # You'll need actual URLs
    with rasterio.open(cog_url) as src:
        data = src.read()