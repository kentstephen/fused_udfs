# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(
    # URL to the NYC Taxi “yellow” data for January 2010 (publicly available on S3)
    path: str = "https://s3.amazonaws.com/nyx-tlc/trip data/yellow_tripdata_2010-01.csv",
    # The map bounds supplied by the Fused UI (min_lng, min_lat, max_lng, max_lat)
    bounds: fused.types.Bounds = None,
    # H3 resolution (default 7 gives ~1 km cells)
    res: int = 7,
):
    """
    Returns a dataframe of H3 hexagons (column `hex`) with a count of
    taxi pickups (`count`) that occurred in January 2010 within the
    current map bounds.
    """
    import pandas as pd

    # ------------------------------------------------------------------
    # 1️⃣  Load and filter the raw CSV (cached for speed)
    # ------------------------------------------------------------------
    @fused.cache
    def load_data(csv_path: str) -> pd.DataFrame:
        # Load the CSV, parse the pickup datetime column
        df = pd.read_csv(
            csv_path,
            parse_dates=["tpep_pickup_datetime"],
            low_memory=False,
        )
        # Keep only the rows from 2010‑01‑01 to 2010‑02‑01
        start = pd.Timestamp("2010-01-01")
        end = pd.Timestamp("2010-02-01")
        df = df[
            (df["tpep_pickup_datetime"] >= start)
            & (df["tpep_pickup_datetime"] < end)
        ]

        # Rename the coordinate columns to a standard name
        df = df.rename(columns={"pickup_longitude": "lng", "pickup_latitude": "lat"})

        # Keep only the columns we need and drop rows with missing coordinates
        df = df[["lat", "lng"]].dropna().copy()

        # Add a constant column that will be summed later to get a count
        df["data"] = 1
        return df

    # Load the data (cached on first run)
    df = load_data(path)

    # ------------------------------------------------------------------
    # 2️⃣  Optional: clip to the current map bounds (if provided)
    # ------------------------------------------------------------------
    if isinstance(bounds, tuple) and len(bounds) == 4:
        min_lng, min_lat, max_lng, max_lat = bounds
        df = df[
            (df["lng"] >= min_lng)
            & (df["lng"] <= max_lng)
            & (df["lat"] >= min_lat)
            & (df["lat"] <= max_lat)
        ]

    # ------------------------------------------------------------------
    # 3️⃣  Aggregate to H3 using the shared utility functions
    # ------------------------------------------------------------------
    # Load the shared utility module (contains `aggregate_df_hex`)
    utils = (
        fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/")
    ).utils

    # `aggregate_df_hex` will:
    #   * convert each point to an H3 cell at the requested resolution
    #   * aggregate the `data` column (here we use `sum` to count points)
    #   * return a dataframe with columns `hex` and `metric`
    result = utils.aggregate_df_hex(
        bounds,          # the same bounds we used for clipping
        df=df,
        res=res,
        stats_type="sum",  # sum of the `data` column = count of points
    )

    # Rename the generic `metric` column to something more explicit
    result = result.rename(columns={"metric": "count"})

    # The returned dataframe will be rendered as an H3HexagonLayer in the UI
    return result