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
    
    # Get SWIR range for a specific time step (to avoid loading entire dataset)
    swir_data = z['swir'][time_slice]
    
    # Calculate range, ignoring NaN/invalid values
    valid_mask = ~np.isnan(swir_data)
    if valid_mask.any():
        swir_min = np.min(swir_data[valid_mask])
        swir_max = np.max(swir_data[valid_mask])
        swir_mean = np.mean(swir_data[valid_mask])
        
        print(f"\nSWIR Statistics (time step {time_slice}):")
        print(f"  Min: {swir_min}")
        print(f"  Max: {swir_max}")
        print(f"  Mean: {swir_mean}")
        print(f"  Valid pixels: {valid_mask.sum()} / {swir_data.size}")
    else:
        print("No valid SWIR data found")
    
    # If you want range across multiple time steps (be careful with memory):
    # sample_indices = np.linspace(0, z['swir'].shape[0]-1, 10, dtype=int)
    # swir_sample = z['swir'][sample_indices]
    # overall_min = np.nanmin(swir_sample)
    # overall_max = np.nanmax(swir_sample)
