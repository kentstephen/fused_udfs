@fused.udf
def udf(roi_wkt:str = '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[-4100000.0, 2400000.0], [3200000.0, 2400000.0], [3200000.0, -1500000.0], [-4100000.0, -1500000.0], [-4100000.0, 2400000.0]]]}}]}',
        crs:str = 'PROJCRS["WGS84 / Lambert_Conformal_Conic_2SP",BASEGEOGCRS["WGS84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1,ID["EPSG",9001]]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]]],CONVERSION["unnamed",METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],PARAMETER["Latitude of false origin",33,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]],PARAMETER["Longitude of false origin",-125,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Latitude of 1st standard parallel",21,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",45,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Easting at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8827]]],CS[Cartesian,2],AXIS["easting",east,ORDER[1],LENGTHUNIT["Meter",1]],AXIS["northing",north,ORDER[2],LENGTHUNIT["Meter",1]]]', 
        partition_str:str = '{"x_start":{"0":1190,"1":2380,"2":3570,"3":1190,"4":2380,"5":3570},"x_stop":{"0":2390,"1":3580,"2":4770,"3":2390,"4":3580,"5":4770},"y_start":{"0":0,"1":0,"2":0,"3":1190,"4":1190,"5":1190},"y_stop":{"0":1200,"1":1200,"2":1200,"3":2390,"4":2390,"5":2390},"fused_index":{"0":0,"1":1,"2":2,"3":3,"4":4,"5":5}}',
        datestr:int='2025-08-05', start_i:int=0, end_i:int=6, band:int=8, product_name='ABI-L2-CMIPF', runner_udf_name='GOES_18_Runner_copy', runner_email='sina@fused.io', min_pixel_value=1500, max_pixel_value=2800, colormap=''):
    start_i=int(start_i); end_i=int(end_i); band=int(band);
    import pandas as pd
    import numpy as np
    
    frames = run_batch(datestr, start_i, end_i, partition_str, roi_wkt, crs, udf_name=runner_udf_name, email=runner_email, colormap=colormap, min_pixel_value=min_pixel_value, max_pixel_value=max_pixel_value, band=band, product_name=product_name)
    
    # Debug: Check what we actually got
    print(f"Total frames returned: {len(frames)}")
    for idx, frame in enumerate(frames[:3]):  # Check first 3
        print(f"Frame {idx} type: {type(frame)}")
        if frame is not None:
            if isinstance(frame, tuple):
                print(f"  Tuple length: {len(frame)}")
                for i, item in enumerate(frame):
                    print(f"    Item {i}: {type(item)}")
                    if hasattr(item, 'shape'):
                        print(f"      Shape: {item.shape}")
            elif hasattr(frame, 'image'):
                print(f"  Has .image attribute")
    
    # Fixed line - handle both tuple and object cases
    frames_raw = []
    for i in frames:
        if i is not None:
            # If it's a tuple, get the first item (the numpy array)
            if isinstance(i, tuple):
                img_data = i[0] if len(i) > 0 else None
                if img_data is not None:
                    # Check if it's a numpy array or has .values attribute
                    if hasattr(img_data, 'values'):
                        data = img_data.values
                    else:
                        # It's already a numpy array
                        data = img_data
                    
                    # Handle different array dimensions
                    if data.ndim == 3:
                        frames_raw.append(data[:, 1:-1, 1:-1])
                    elif data.ndim == 2:
                        frames_raw.append(data[1:-1, 1:-1])
                    else:
                        print(f"Warning: Unexpected array dimension {data.ndim}, shape {data.shape}")
                        frames_raw.append(data)
                        
            # If it has .image attribute
            elif hasattr(i, 'image'):
                if hasattr(i.image, 'values'):
                    data = i.image.values
                else:
                    data = i.image
                    
                if data.ndim == 3:
                    frames_raw.append(data[:, 1:-1, 1:-1])
                elif data.ndim == 2:
                    frames_raw.append(data[1:-1, 1:-1])
                else:
                    frames_raw.append(data)
                    
            # If it's already the image array/data
            elif hasattr(i, 'values'):
                data = i.values
                if data.ndim == 3:
                    frames_raw.append(data[:, 1:-1, 1:-1])
                elif data.ndim == 2:
                    frames_raw.append(data[1:-1, 1:-1])
                else:
                    frames_raw.append(data)
            else:
                # Assume it's a numpy array directly
                if i.ndim == 3:
                    frames_raw.append(i[:, 1:-1, 1:-1])
                elif i.ndim == 2:
                    frames_raw.append(i[1:-1, 1:-1])
                else:
                    frames_raw.append(i)
    
    print(f"Successfully processed {len(frames_raw)} frames")
    
    if len(frames_raw) == 0:
        raise ValueError("No valid frames were processed. Check the debug output above.")
    
    frames_all = np.stack(frames_raw) 
    df = pd.DataFrame({'arr': [frames_all.flatten()]})
    df['shape'] = [frames_all.shape] 
    return df

import fused

def run_async(fn, arr_args):
    import asyncio
    import nest_asyncio
    nest_asyncio.apply()    
    a = []
    for i in arr_args:
        a.append(asyncio.to_thread(fn, i))
    async def main(): 
        return await asyncio.gather(*a)
    return asyncio.run(main())
 
def runner(params):
    try:
        i = params['i'] 
        datestr = params['datestr'] 
        band = params['band']
        product_name = params['product_name']
        roi_wkt = params['roi_wkt']
        crs = params['crs']
        partition_str = params['partition_str']
        udf_name = params['udf_name']
        email = params['email']
        colormap = params['colormap']
        min_pixel_value = params['min_pixel_value']
        max_pixel_value = params['max_pixel_value']
        
        result = fused.run('fsh_6vmgKG6mGbjrlESNeYxO4c', 
                          product_name=product_name, 
                          i=i, 
                          datestr=datestr, 
                          colormap=colormap, 
                          min_pixel_value=min_pixel_value, 
                          max_pixel_value=max_pixel_value, 
                          band=band, 
                          roi_wkt=roi_wkt, 
                          crs=crs, 
                          partition_str=partition_str)
        return result
    except Exception as e:
        print(f"Error in runner for i={params.get('i')}: {e}")
        return None

def run_batch(datestr, start_i, end_i, partition_str, roi_wkt, crs, udf_name, email, colormap, min_pixel_value, max_pixel_value, band=8, product_name='ABI-L2-CMIPF'):
    arg_list = [{'i': i, 'datestr': datestr, 'band': band, 'product_name': product_name, 'crs': crs, 'roi_wkt': roi_wkt, 'partition_str': partition_str, 'udf_name': udf_name, 'email': email, 'colormap': colormap, "min_pixel_value": min_pixel_value, "max_pixel_value": max_pixel_value} 
                for i in range(start_i, end_i)]
    return run_async(runner, arg_list)