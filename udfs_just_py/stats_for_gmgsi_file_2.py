@fused.udf(cache_max_age="0s")
def udf(bounds: fused.types.Bounds = None, time_slice: int = -1):
    import icechunk
    import zarr
    import numpy as np
    from datetime import datetime
    
    print("=" * 60)
    print("CONNECTING TO ICECHUNK REPOSITORY")
    print("=" * 60)
    
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
    
    print("✓ Successfully opened Zarr group\n")
    
    # Dataset structure overview
    print("=" * 60)
    print("DATASET STRUCTURE")
    print("=" * 60)
    print(f"Available variables: {list(z.keys())}")
    
    if 'vis' in z:
        print(f"\nVIS data shape: {z['vis'].shape}")
        print(f"  • Dimensions: {len(z['vis'].shape)}D")
        print(f"  • Time steps: {z['vis'].shape[0]}")
        if len(z['vis'].shape) > 1:
            print(f"  • Spatial dimensions: {z['vis'].shape[1:]}")
    
    # Time coordinate analysis
    print("\n" + "=" * 60)
    print("TIME COORDINATE ANALYSIS")
    print("=" * 60)
    
    if 'time' in z:
        times = z['time'][:]
        print(f"Time array shape: {times.shape}")
        print(f"Time data type: {times.dtype}")
        print(f"Total timestamps: {len(times)}")
        
        # Analyze timestamp format
        print(f"\nRaw timestamp values:")
        print(f"  • First: {times[0]}")
        print(f"  • Last: {times[-1]}")
        print(f"  • Sample (first 3): {times[:3].tolist()}")
        
        # Attempt conversion if numeric
        if np.issubdtype(times.dtype, np.integer) or np.issubdtype(times.dtype, np.floating):
            print(f"\n🔍 Attempting timestamp conversion...")
            
            # Try nanoseconds
            try:
                first_dt = datetime.fromtimestamp(times[0] / 1e9)
                last_dt = datetime.fromtimestamp(times[-1] / 1e9)
                print(f"  ✓ Nanoseconds interpretation:")
                print(f"    • First: {first_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"    • Last: {last_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"    • Time span: {(last_dt - first_dt).days} days")
            except (ValueError, OSError):
                print(f"  ✗ Not nanoseconds")
            
            # Try milliseconds
            try:
                first_dt = datetime.fromtimestamp(times[0] / 1e3)
                last_dt = datetime.fromtimestamp(times[-1] / 1e3)
                print(f"  ✓ Milliseconds interpretation:")
                print(f"    • First: {first_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"    • Last: {last_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"    • Time span: {(last_dt - first_dt).days} days")
            except (ValueError, OSError):
                print(f"  ✗ Not milliseconds")
                
            # Try seconds
            try:
                first_dt = datetime.fromtimestamp(times[0])
                last_dt = datetime.fromtimestamp(times[-1])
                print(f"  ✓ Seconds interpretation:")
                print(f"    • First: {first_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"    • Last: {last_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"    • Time span: {(last_dt - first_dt).days} days")
            except (ValueError, OSError):
                print(f"  ✗ Not seconds")
    else:
        print("⚠ No 'time' variable found in dataset")
    
    # Metadata exploration
    print("\n" + "=" * 60)
    print("METADATA")
    print("=" * 60)
    
    if hasattr(z, 'attrs') and z.attrs:
        print("Group-level attributes:")
        for key, value in z.attrs.items():
            print(f"  • {key}: {value}")
    else:
        print("No group-level attributes found")
    
    if 'vis' in z and hasattr(z['vis'], 'attrs') and z['vis'].attrs:
        print("\nVIS variable attributes:")
        for key, value in z['vis'].attrs.items():
            print(f"  • {key}: {value}")
    
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)