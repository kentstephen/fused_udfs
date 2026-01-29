import fused
import requests
from io import BytesIO
from PIL import Image

@fused.udf
def udf(bbox: fused.types.TileGDF = None, n: int = 10):
    # Extract tile coordinates (x, y, z) from the bounding box
    x, y, z = bbox[["x", "y", "z"]].iloc[0]

    # Construct the URL for the tile
    tile_url = f"https://a.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}@2x.png"

    # Fetch the tile image
    response = requests.get(tile_url)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch tile: {response.status_code}")

    # Load the image using PIL (Python Imaging Library)
    tile_image = Image.open(BytesIO(response.content))

    # Convert the image to a byte stream in PNG format
    output = BytesIO()
    tile_image.save(output, format="PNG")
    output.seek(0)

    # Return the raw PNG data
    return output.getvalue()