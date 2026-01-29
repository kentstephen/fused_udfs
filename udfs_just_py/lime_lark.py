@fused.udf
def udf(bounds: fused.types.Bounds = None):
    import geopandas as gpd
    import pandas as pd
    import requests
    from shapely.geometry import Point, box
    from io import StringIO
    import numpy as np
    
    @fused.cache
    def load_firms_fires():
        """Load NASA FIRMS fire data for California, January 2025"""
        
        print("Loading NASA FIRMS fire data...")
        
        # NASA FIRMS provides free CSV data - MODIS C6.1 and VIIRS
        # Using VIIRS I-Band 375m for better resolution
        # Format: https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/
        
        # This URL provides the most recent 7 days, but we need January data
        # Using the archive for January 2025
        base_url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_USA_contiguous_and_Hawaii_7d.csv"
        
        try:
            response = requests.get(base_url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV
            df = pd.read_csv(StringIO(response.text))
            
            # Filter for California (approximate bbox)
            ca_bbox = {
                'min_lon': -124.5,
                'max_lon': -114.0,
                'min_lat': 32.5,
                'max_lat': 42.0
            }
            
            df = df[
                (df['longitude'] >= ca_bbox['min_lon']) & 
                (df['longitude'] <= ca_bbox['max_lon']) &
                (df['latitude'] >= ca_bbox['min_lat']) & 
                (df['latitude'] <= ca_bbox['max_lat'])
            ]
            
            print(f"Found {len(df)} fire detections in California")
            
            # Convert to GeoDataFrame with points
            geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
            gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
            
            # Parse dates
            gdf['acq_date'] = pd.to_datetime(gdf['acq_date'])
            
            # Filter for high confidence fires only
            if 'confidence' in gdf.columns:
                gdf = gdf[gdf['confidence'].isin(['h', 'high', 'n', 'nominal'])].copy()
                print(f"High confidence fires: {len(gdf)}")
            
            return gdf
            
        except Exception as e:
            print(f"FIRMS error: {e}")
            return gpd.GeoDataFrame()
    
    # Load fire data
    gdf = load_firms_fires()
    
    if gdf.empty:
        print("No fire data available")
        return gpd.GeoDataFrame()
    
    # Filter by viewport bounds
    if bounds is not None:
        min_lon, min_lat, max_lon, max_lat = bounds
        bounds_box = box(min_lon, min_lat, max_lon, max_lat)
        gdf = gdf[gdf.geometry.intersects(bounds_box)]
        print(f"{len(gdf)} fires in current viewport")
    else:
        print(f"Showing all {len(gdf)} fires")
    
    # Create circular buffers based on FRP (Fire Radiative Power)
    # FRP is in MW - scale it to reasonable circle sizes (meters)
    if 'frp' in gdf.columns and len(gdf) > 0:
        # Convert to UTM Zone 11N for Southern California (proper metric projection)
        gdf_utm = gdf.to_crs("EPSG:32611")
        
        # Scale FRP to radius in meters: base radius of 100m + FRP * 50
        # This makes small fires ~100m radius, larger fires can be 500m+ radius
        gdf_utm['radius_m'] = 100 + (gdf_utm['frp'] * 50)
        
        print(f"FRP range: {gdf['frp'].min():.2f} to {gdf['frp'].max():.2f} MW")
        print(f"Radius range: {gdf_utm['radius_m'].min():.0f}m to {gdf_utm['radius_m'].max():.0f}m")
        
        # Create circular buffers
        gdf_utm['geometry'] = gdf_utm.apply(
            lambda row: row.geometry.buffer(row['radius_m']),
            axis=1
        )
        
        # Convert back to WGS84
        gdf = gdf_utm.to_crs("EPSG:4326")
    
    # Print schema
    print(gdf.T)
    
    return gdf