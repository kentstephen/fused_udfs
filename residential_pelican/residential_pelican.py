@fused.udf
def udf(bounds: fused.types.Bounds, preview: bool = False):
    import imageio.v3 as iio
    import s3fs
    print(bounds)
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tiles = common.get_tiles(bounds)
    print(tiles)
    x, y, z = tiles.iloc[0][["x", "y", "z"]]
    path = f"s3://elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
    print(path)
    # int(path)
    
    # load dem
    @fused.cache
    def load(bounds):
        with s3fs.S3FileSystem().open(path) as f:
            im = iio.imread(f)  # Shape: (256, 256, 3) - height, width, RGB
            
            # Decode terrarium format: (R * 256 + G + B / 256) - 32768
            r, g, b = im[:, :, 0], im[:, :, 1], im[:, :, 2]
            elevation = (r * 256 + g + b / 256) - 32768
            
            # Now elevation is (256, 256) with single elevation values in meters
            print(f"Elevation shape: {elevation.shape}")
            print(f"Elevation range: {elevation.min():.2f}m to {elevation.max():.2f}m")
            
            if preview:
                w, h = elevation.shape[1], elevation.shape[0]
                if w > h:
                    return (elevation, (0, 0, 1, 1 / (w / h)))
                else:
                    return (elevation, (0, 0, 1 / (h / w), 1))
            return elevation
    
    arr = load(bounds)
    return arr.astype("uint8")