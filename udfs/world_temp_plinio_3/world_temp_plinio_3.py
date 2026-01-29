@fused.udf
def udf(datestr: str = "2025-01-01", res: int = 4, var: str = "t2m", return_format: str = "raster"):
    import pandas as pd
    import h3
    import xarray
    import numpy as np
    import pyarrow as pa
    import fused  # added import for fused utilities

    # Attempt to import rasterization utilities; if unavailable, fallback to H3 dataframe
    try:
        from h3ronpy.pandas.raster import rasterize_cells  # type: ignore
        raster_available = True
    except Exception:
        raster_available = False

    # Load ERA5 NetCDF file from GCS
    path_in = f"https://storage.googleapis.com/gcp-public-data-arco-era5/raw/date-variable-single_level/{datestr.replace('-', '/')}/2m_temperature/surface.nc"
    path = fused.download(path_in, path_in)
    xds = xarray.open_dataset(path)

    # Convert variable to DataFrame with lat/lon index
    df = xds[var].to_dataframe().unstack(0)
    df.columns = df.columns.droplevel(0)

    # Add H3 index column
    df["hex"] = df.index.map(lambda x: h3.api.basic_int.latlng_to_cell(x[0], x[1], res))
    df = df.set_index("hex").sort_index()

    # Rename hour columns and compute a metric
    df.columns = [f"hour{hr}" for hr in range(24)]
    df["daily_mean"] = df.iloc[:, :24].values.mean(axis=1)
    df["metric"] = df["hour1"]

    if return_format == "raster":
        if not raster_available:
            # Fallback: return the H3 dataframe if raster tools are missing
            return df.reset_index()[["hex", "metric"]]

        # Convert H3 to raster for smooth animation
        df_out = df[["metric"]].reset_index().rename(columns={"hex": "h3index"})

        size = 1000  # raster resolution; adjust as needed
        nodata_value = -9999

        array, transform = rasterize_cells(
            pa.array(df_out["h3index"]),
            pa.array(df_out["metric"].astype("float32")),
            size,
            nodata_value=nodata_value,
        )
        return {"array": array, "transform": transform, "nodata": nodata_value}
    else:
        # Return H3 format for direct visualization
        return df.reset_index()[["hex", "metric"]]