# SAR_Umbra_File_Example_copy

# Overview

Umbra satellites provide high-resolution Synthetic Aperture Radar (SAR) imagery, with up to 16-cm resolution, capable of capturing images at night and in adverse weather conditions. 

# External links

- [stac link](https://radiantearth.github.io/stac-browser/#/external/s3.us-west-2.amazonaws.com/umbra-open-data-catalog/stac/catalog.json)
- [aws open data](https://registry.opendata.aws/umbra-open-data/)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/SAR_Umbra_File_Example")
arr = fused.run(udf=udf)
arr.image.plot()
```


