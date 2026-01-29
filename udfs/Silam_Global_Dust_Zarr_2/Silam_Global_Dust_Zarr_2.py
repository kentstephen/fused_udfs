@fused.udf
def udf(
    bounds: fused.types.Bounds = [-180, -90, 180, 90],
    layer: str = 'total_cloud_cover_atmosphere',
    min_max = (0, 3000),
    year: int = 2025,
    month: int = 10,
    day: int = 27,
    step: int = 80,
):
    """UDF to visualize a dust-concentration layer from the SILAM Zarr dataset."""

    import numpy as np
    import zarr

    # Common visualization helpers
    common_utils = fused.load(
        "https://github.com/fusedio/udfs/tree/abf9c87/public/common/"
    )

    # Open the remote Zarr store (read-only, anonymous)
    store = zarr.storage.FsspecStore.from_url(
        "s3://us-west-2.opendata.source.coop/dynamical/noaa-hrrr-forecast-48-hour/v0.1.0.zarr",
        read_only=True,
        storage_options={"anon": True},
    )
    z = zarr.open_group(store=store, mode="r")

    # Latitude / longitude vectors (1-D)
    lat = z["latitude"][:]
    lon = z["longitude"][:]

    # Lead-time array
    lead_times = z["lead_time"][:]

    print(f"Requested bounds: {bounds}")

    # ------------------------------------------------------------------
    # Validate that requested bounds intersect the dataset coverage
    # (HRRR covers roughly lat 20-55°N, lon -130--60°W)
    # ------------------------------------------------------------------
    if max(b[1] for b in [bounds]) < lat.min() or min(b[1] for b in [bounds]) > lat.max() \
       or max(b[0] for b in [bounds]) < lon.min() or min(b[0] for b in [bounds]) > lon.max():
        return "<p>Selected region is outside HRRR data coverage (North America only).</p>"

    # Clip bounds to the actual data extent
    minx, miny, maxx, maxy = bounds
    miny = max(miny, float(lat.min()))
    maxy = min(maxy, float(lat.max()))
    minx = max(minx, float(lon.min()))
    maxx = min(maxx, float(lon.max()))

    # ------------------------------------------------------------------
    # Spatial index selection
    # ------------------------------------------------------------------
    lat_mask = (lat >= miny) & (lat <= maxy)
    lon_mask = (lon >= minx) & (lon <= maxx)

    lat_idx = np.where(lat_mask)[0]
    lon_idx = np.where(lon_mask)[0]

    # Guard against empty selections
    if lat_idx.size == 0 or lon_idx.size == 0:
        print("No latitude/longitude indices found after clipping.")
        return "<p>No data available for the selected geographic bounds.</p>"

    # ------------------------------------------------------------------
    # Time index selection – pick the lead_time nearest to `step`
    # ------------------------------------------------------------------
    if step < 0 or step >= lead_times.shape[0]:
        # fallback to the first available time step
        time_idx = 0
        print(f"Requested step {step} out of range, using step {time_idx}.")
    else:
        time_idx = int(step)  # `step` is already an integer index for HRRR (hourly)
        # If `step` is meant to be a lead-time value rather than an index,
        # uncomment the line below to select the nearest value:
        # time_idx = int(np.argmin(np.abs(lead_times - step)))

    print(f"Using time index {time_idx}, lead_time={lead_times[time_idx]}")

    # ------------------------------------------------------------------
    # Extract the data slice (time, lat, lon)
    # ------------------------------------------------------------------
    var_arr = z[layer][
        time_idx,
        lat_idx.min() : lat_idx.max() + 1,
        lon_idx.min() : lon_idx.max() + 1,
    ]

    # If the slice is empty, avoid visualisation
    if var_arr.size == 0:
        print("Extracted variable slice is empty.")
        return "<p>No data extracted for this region and time.</p>"

    da = var_arr  # no additional scaling for now

    print(f"Data slice shape: {da.shape}")

    # ------------------------------------------------------------------
    # Visualization preparation
    # ------------------------------------------------------------------
    bins = [
        0, 3, 6, 15, 30, 60, 150, 300, 600, 1500, 3000, np.inf,
    ]
    colors = [
        "#0000FF", "#008080", "#66CDAA", "#006400", "#90EE90",
        "#FFFF00", "#FFA500", "#FF8C00", "#FF0000", "#C71585", "#C71585",
    ]

    # Flip vertically so north is up (Zarr stores latitude decreasing)
    data_array = np.flipud(da)

    # Replace NaNs with 0 for visualization
    masked_data = np.nan_to_num(data_array, nan=0)

    # Simple mask – values above the first bin threshold
    mask = data_array >= bins[1]

    # Generate the raster HTML via the common helper
    viz = common_utils.visualize(
        data=masked_data,
        mask=mask,
        min=min_max[0],
        max=min_max[1],
        colormap=colors,
        colorbins=bins,
    )

    return viz