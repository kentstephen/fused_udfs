# H3_Hexagon_Layer_Example

## Overview

This UDF shows how to open NYC yellow taxi trip dataset using DuckDB and aggregate the pickups using [H3-DuckDB](https://github.com/isaacbrodsky/h3-duckdb). Results are visualized as [hexagons](https://deck.gl/docs/api-reference/geo-layers/h3-hexagon-layer).

## External links

- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/H3_Hexagon_Layer_Example")
gdf = fused.run(udf=udf)
gdf
```


