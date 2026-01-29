@fused.udf(cache_max_age="0s")
def udf(bounds: fused.types.Bounds = None, time_slice: int = -1):
    import icechunk
    import zarr
    import numpy as np
    from datetime import datetime
    
    storage = icechunk.s3_storage(
        bucket="us-west-2.opendata.source.coop",
        prefix="bkr/gmgi/gmgsi_v3.icechunk",
        region="us-west-2",
        anonymous=True
    )
    
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")
    store = session.store
    z = zarr.open_group(store=store, mode='r')
    
    # Get time information
    print(f"Dataset shape: {z['vis'].shape}")
    print(f"Total time steps: {z['vis'].shape[0]}")
    
    # Check if there's a time coordinate variable
    print("\nAvailable variables:", list(z.keys()))
    # print(z['freq'])
    # Try to get time metadata
    if 'time' in z:
        times = z['time'][:]
        print(f"\nFirst timestamp: {times[0]}")
        print(f"Last timestamp: {times[-1]}")
        print(f"Time dtype: {times.dtype}")
        
        # If it's a numeric timestamp, try to convert
        if np.issubdtype(times.dtype, np.integer) or np.issubdtype(times.dtype, np.floating):
            # Might be Unix timestamp or days since epoch
            print(f"\nFirst few time values: {times[:5]}")
            print(f"Last few time values: {times[-5:]}")
    
    # Check for time attributes in the vis variable
    if hasattr(z['vis'], 'attrs'):
        print("\nVis attributes:", dict(z['vis'].attrs))
    
    # Check group-level attributes
    if hasattr(z, 'attrs'):
        print("\nGroup attributes:", dict(z.attrs))
    
    # return {"time_steps": z['vis'].shape[0], "keys": list(z.keys())}