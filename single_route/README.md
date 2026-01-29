# single_route

## Overview

This UDF returns a single optimal (time minimized) route as a function of origin and destination coordinate pairs (input variables), using the Valhalla routing engine API. The geopandas dataframe returned contains three rows: start and end point geometries and the linestring geometry of the optimal path. (Simple driving directions are also printed.)

## External links

- [Valhalla GitHub Docs](https://valhalla.github.io/valhalla/)


