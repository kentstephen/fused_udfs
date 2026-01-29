# DuckDB_H3_Example_for_nyc_builidngs

## Overview

This UDF shows how to open NYC yellow taxi trip dataset using DuckDB and aggregate the pickups using [H3-DuckDB](https://github.com/isaacbrodsky/h3-duckdb).

## External links

- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/DuckDB_H3_Example")
gdf = fused.run(udf=udf)
gdf
```


