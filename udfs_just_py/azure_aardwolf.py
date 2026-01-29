utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
@fused.udf(max_cache_age='0s')
def udf(bounds: fused.types.Bounds, taxon_id:int=119669, res:int=7):
    from utils import aggregate_df_hex, df_to_hex
    import pandas as pd
    import numpy as np
    from scipy.ndimage import label, center_of_mass
    from PIL import Image
    import io
    
    # Get PNG data
    png_data = fused.run("fsh_44Oli4PnJgjrT7JwSUD6NZ", bounds=bounds, taxon_id=taxon_id)
    
    # Debug: Check what type of object we got
    print(f"PNG data type: {type(png_data)}")
    print(f"PNG data shape/length: {len(png_data) if hasattr(png_data, '__len__') else 'No length'}")
    
    # Extract bytes from the PNG data - try different approaches
    if isinstance(png_data, bytes):
        image_bytes = png_data
    elif hasattr(png_data, 'values') and callable(png_data.values):
        image_bytes = png_data.values()
    elif hasattr(png_data, 'values'):
        image_bytes = png_data.values
    elif isinstance(png_data, (pd.DataFrame, pd.Series)):
        # If it's a DataFrame/Series, get the first value
        image_bytes = png_data.iloc[0] if len(png_data) > 0 else png_data.values[0]
    elif isinstance(png_data, np.ndarray):
        image_bytes = png_data.tobytes() if png_data.dtype == np.uint8 else bytes(png_data)
    else:
        # Last resort - try to convert to bytes
        image_bytes = bytes(png_data)
    
    # Convert to grayscale array
    image = Image.open(io.BytesIO(image_bytes)).convert('L')
    arr = np.array(image)
    
    # Threshold to find circles (adjust threshold as needed)
    mask = arr > 50  # pixels above background
    
    # Find connected components and their centroids
    labeled, num_circles = label(mask)
    centroids = center_of_mass(mask, labeled, range(1, num_circles + 1))
    
    # Convert pixel centroids to lat/lng
    img_height, img_width = arr.shape
    
    coords = []
    for y, x in centroids:
        lng = bounds[0] + (x / img_width) * (bounds[2] - bounds[0])
        lat = bounds[3] - (y / img_height) * (bounds[3] - bounds[1])
        coords.append({'lat': lat, 'lng': lng, 'data': 1})
    
    df = pd.DataFrame(coords)
    
    # Convert to H3
    df = df_to_hex(bounds, df=df, res=res)
    
    return df