def shape_transform_to_xycoor(shape, transform):
    import numpy as np

    n_y = shape[0]  # Fixed: was shape[-2]
    n_x = shape[1]  # Fixed: was shape[-1]
    w, _, x, _, h, y, _, _, _ = transform
    x_list = np.arange(x + w / 2, x + n_x * w + w / 2, w)[:n_x]
    y_list = np.arange(y + h / 2, y + n_y * h + h / 2, h)[:n_y]
    return x_list, y_list

def arr_to_latlng(arr, bounds):
    import numpy as np
    import pandas as pd
    from rasterio.transform import from_bounds
    from pyproj import Transformer
    
    transform = from_bounds(*bounds, arr.shape[-1], arr.shape[-2])
    x_list, y_list = shape_transform_to_xycoor(arr.shape[-2:], transform)
    X, Y = np.meshgrid(x_list, y_list)
    
    # Transform coordinates from 3857 to 4326
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    lngs, lats = transformer.transform(X.flatten(), Y.flatten())
    
    df = pd.DataFrame({
        "lng": lngs,
        "lat": lats,
        "data": arr.flatten()
    })
    return df