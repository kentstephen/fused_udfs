# After stacking
ds = stackstac.stack(
    items,
    assets=[band],
    epsg=3857,
    bounds=bounds,
    resolution=resolution,
).squeeze()

# Debug the dataset structure
print(f"Dataset variables: {list(ds.data_vars)}")
print(f"Dataset dimensions: {ds.dims}")

# Try to access the band data safely
if band in ds:
    arr = ds[band]
    if 'time' in arr.dims and arr.dims['time'] > 1:
        arr = arr.max(dim="time")
elif band in ds.data_vars:
    arr = ds[band]
    if 'time' in arr.dims and arr.dims['time'] > 1:
        arr = arr.max(dim="time")
else:
    # Fallback to first available data variable
    if len(ds.data_vars) > 0:
        first_var = list(ds.data_vars)[0]
        print(f"Band '{band}' not found, using '{first_var}' instead")
        arr = ds[first_var]
        if 'time' in arr.dims and arr.dims['time'] > 1:
            arr = arr.max(dim="time")
    else:
        print("No data variables found in dataset")
        return