# utils.py
import numpy as np
import h3
import geopandas as gpd
from shapely.geometry import Polygon
from collections import defaultdict
import rasterio
from rasterio.transform import from_origin
from rasterio.transform import from_bounds

def pixel_to_latlon(x, y, transform):
    """Convert pixel coordinates to latitude and longitude."""
    lon, lat = rasterio.transform.xy(transform, y, x)
    return lat, lon

def raster_to_h3_indices(output, transform, resolution):
    rows, cols = output.shape[1], output.shape[2]
    
    # Generate lat/lon for each pixel
    latlon_array = np.array([[pixel_to_latlon(x, y, transform) for x in range(cols)] for y in range(rows)])
    
    h3_indices = []
    values = []
    for y in range(output.shape[1]):
        for x in range(output.shape[2]):
            lat, lon = latlon_array[y, x]
            h3_index = h3.latlng_to_cell(lat, lon, resolution)
            h3_indices.append(h3_index)
            values.append(output[:, y, x])
    
    h3_data = defaultdict(list)
    for h3_index, value in zip(h3_indices, values):
        h3_data[h3_index].append(value)
    
    h3_aggregated = {h3_index: np.mean(values, axis=0) for h3_index, values in h3_data.items()}
    
    return h3_aggregated

def h3_to_geodataframe(h3_aggregated):
    polygons = []
    values = []
    for h3_index, value in h3_aggregated.items():
        polygon = h3.cell_to_boundary(h3_index)
        polygon = Polygon(polygon)
        polygons.append(polygon)
        values.append(value.tolist())
    
    gdf = gpd.GeoDataFrame({'geometry': polygons, 'value': values})
    return gdf
def get_transform_from_bbox(bbox, raster_shape):
    left, bottom, right, top = bbox.total_bounds
    height, width = raster_shape
    transform = from_bounds(left, bottom, right, top, width, height)
    return transform
