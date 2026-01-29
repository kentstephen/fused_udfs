@fused.udf
def udf(bounds: fused.types.Bounds, n: int = 10):
    import fused
    import requests
    from io import BytesIO
    from PIL import Image
    
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds)
    
    # Get x, y, z from the tile
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    
    # DEBUG: Print what we're requesting
    print(f"Requesting tile: z={z}, x={x}, y={y}")
    print(f"Bounds: {bounds}")
    
    # Construct the URL for the tile
    tile_url = f"https://a.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}@2x.png"
    
    try:
        response = requests.get(tile_url, timeout=5)
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            tile_image = Image.open(BytesIO(response.content))
            output = BytesIO()
            tile_image.save(output, format="PNG")
            output.seek(0)
            return output.getvalue()
        else:
            return None
            
    except Exception as e:
        print(f"Error: {e}")
        return None