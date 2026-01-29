import fused
import boto3

@fused.udf
def udf(n=10):
    import ee
    import xarray
    import io
    import json
    import dotenv
    from dotenv import load_dotenv
    load_dotenv()

    # Load common utilities from Fused
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils

    # Path to the credentials file
    key_path = /mnt/c/users/skent/~/.config/gee_creds/credentials.json

    # Authenticate GEE using the temporary file path
    credentials = ee.ServiceAccountCredentials(
        "fused-account@fused-gee.iam.gserviceaccount.com",
        key_path
    )
    ee.Initialize(credentials)

    # Define a bounding box for New York State
    ny_bbox = ee.Geometry.Rectangle([-79.7624, 40.4774, -71.1851, 45.0159])

    # Load data from a GEE ImageCollection filtered by the New York bbox
    ic = ee.ImageCollection("projects/sat-io/open-datasets/CISI/global_CISI").filterBounds(ny_bbox)

    # Example operation: Get the mean of the images in the collection over New York State
    mean_image = ic.mean()

    # Convert the clipped image to an xarray Dataset
    # Note: 'xee' package might be necessary to use xarray with 'engine="ee"'
    # Example of opening a dataset (ensure this approach works in your environment)
    ds = xarray.open_dataset(mean_image, engine="ee", region=ny_bbox)

    # Optionally, convert xarray Dataset to numpy array
    array = ds.to_array().values

    return array

# Example to run the UDF
# arr = fused.run(udf=udf)
