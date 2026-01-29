@fused.udf                                                                                                                                 
def udf(                                                                                                                                   
      bounds: fused.types.Bounds = None,                                                                                                     
      min_val: float = 0.0,                                                                                                                  
      max_val: float = 0.15,                                                                                                                 
      chip_len: int = 512,                                                                                                                   
      mask_threshold: float = 0,                                                                                                             
  ):                                                                                                                                         
      import zarr                                                                                                                            
      import numpy as np                                                                                                                     
      from scipy import ndimage                                                                                                              
      import xarray as xr                                                                                                                    
                                                                                                                                             
      common = fused.load("https://github.com/fusedio/udfs/tree/main/public/common/")                                                        
                                                                                                                                             
      xmin, ymin, xmax, ymax = bounds                                                                                                        
      zoom = common.estimate_zoom(bounds)                                                                                                    
                                                                                                                                             
      pyramid_level = max(0, min(zoom - 1 + 4, 12))                                                                                          
      print(f"Zoom: {zoom}, Pyramid level: {pyramid_level}")                                                                                 
                                                                                                                                             
      store = zarr.open(                                                                                                                     
          "https://carbonplan-share.s3.us-west-2.amazonaws.com/zarr-layer-examples/13-lvl-30m-4326-scott-BP.zarr",                           
          mode='r'                                                                                                                           
      )                                                                                                                                      
      level = store[str(pyramid_level)]                                                                                                      
                                                                                                                                             
      lat = level['latitude'][:]                                                                                                             
      lon = level['longitude'][:]                                                                                                            
                                                                                                                                             
      data_lon_min, data_lon_max = lon.min(), lon.max()                                                                                      
      data_lat_min, data_lat_max = lat.min(), lat.max()                                                                                      
                                                                                                                                             
      if xmax < data_lon_min or xmin > data_lon_max or ymax < data_lat_min or ymin > data_lat_max:                                           
          print(f"Bounds outside data extent")                                                                                               
          return None                                                                                                                        
                                                                                                                                             
      xmin_clamped = max(xmin, data_lon_min)                                                                                                 
      xmax_clamped = min(xmax, data_lon_max)                                                                                                 
      ymin_clamped = max(ymin, data_lat_min)                                                                                                 
      ymax_clamped = min(ymax, data_lat_max)                                                                                                 
                                                                                                                                             
      lon_idx = np.where((lon >= xmin_clamped) & (lon <= xmax_clamped))[0]                                                                   
      lat_idx = np.where((lat >= ymin_clamped) & (lat <= ymax_clamped))[0]                                                                   
                                                                                                                                             
      if len(lon_idx) == 0 or len(lat_idx) == 0:                                                                                             
          return None                                                                                                                        
                                                                                                                                             
      arr = level['BP'][lat_idx.min():lat_idx.max()+1, lon_idx.min():lon_idx.max()+1]                                                        
      arr = np.array(arr, dtype=np.float32)                                                                                                  
                                                                                                                                             
      print(f"Data shape: {arr.shape}, range: [{np.nanmin(arr):.4f}, {np.nanmax(arr):.4f}]")                                                 
                                                                                                                                             
      if arr.size == 0:                                                                                                                      
          return None                                                                                                                        
                                                                                                                                             
      arr = np.flipud(arr)                                                                                                                   
                                                                                                                                             
      # Get actual coordinates for this slice                                                                                                
      lon_slice = lon[lon_idx.min():lon_idx.max()+1]                                                                                         
      lat_slice = lat[lat_idx.min():lat_idx.max()+1][::-1]  # flip to match arr                                                              
                                                                                                                                             
      # Return as xarray DataArray with coordinates                                                                                          
      da = xr.DataArray(                                                                                                                     
          arr,                                                                                                                               
          dims=['y', 'x'],                                                                                                                   
          coords={                                                                                                                           
              'y': lat_slice,                                                                                                                
              'x': lon_slice,                                                                                                                
          }                                                                                                                                  
      )                                                                                                                                      
      return da  