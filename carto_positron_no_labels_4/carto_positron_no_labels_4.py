import fused
import requests
from io import BytesIO
from PIL import Image

@fused.udf
def udf(bounds: fused.types.Bounds, n: int = 10):
    # Extract tile coordinates (x, y, z) from the bounding box
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)
    zoom = common_utils.estimate_zoom(bounds)
    # 1. Initial parameters
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    @fused.cache
    def query(x, y, z):
    # Construct the URL for the tile
        tile_url = f"https://a.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}@2x.png"
    
        # Fetch the tile image
        response = requests.get(tile_url)
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch tile: {response.status_code}")
    
        # Load the image using PIL (Python Imaging Library)
        return Image.open(BytesIO(response.content))
    tile_image = query(x, y , z)
    # Convert the image to a byte stream in PNG format
    output = BytesIO()
    tile_image.save(output, format="PNG")
    output.seek(0)

    # Return the raw PNG data
    return output.getvalue()