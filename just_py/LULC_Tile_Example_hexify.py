@fused.udf
def udf(bounds: fused.types.Tile, context, year="2022", chip_len:int=256):
    import numpy as np
    import pandas as pd
    x, y, z = bounds.iloc[0][["x", "y", "z"]]
    res_offset = 1  # lower makes the hex finer
   # x res = max(min(int(3 + bounds.z[0] / 1.5), 12) - res_offset, 2)
    res =9
    print(res)
    
    # Define the missing dictionaries
    lulc_category_dict = {
        1: "Water",
        2: "Trees",
        4: "Flooded vegetation",
        5: "Crops",
        7: "Built area",
        8: "Bare ground",
        9: "Snow",
        10: "Clouds",
        11: "Rangeland"
    }
    
    if bounds.z[0] >= 5:
        from utils import (
            LULC_IO_COLORS,
            arr_to_color,
            bounds_stac_items,
            mosaic_tiff,
            s3_to_https,
            arr_to_h3,
            rgb_to_hex
        )
        
        # Use LULC_IO_COLORS as your color map
        color_map = LULC_IO_COLORS
        
        try:
            matching_items = bounds_stac_items(
                bounds.geometry[0], table="s3://fused-asset/lulc/io10m/"
            )
            
            mask = matching_items["datetime"].map(lambda x: str(x)[:4] == year)
            tiff_list = (
                matching_items[mask]
                .assets.map(lambda x: s3_to_https(x["supercell"]["href"][:-17] + ".tif"))
                .values
            )
            data = mosaic_tiff(
                bounds,
                tiff_list,
                output_shape=(chip_len, chip_len),
                overview_level=min(max(12 - bounds.z[0], 0), 4),
            )
            
            df = arr_to_h3(data, bounds.total_bounds, res=res, ordered=False)
            df['most_freq'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[0][np.argmax(np.unique(x, return_counts=True)[1])])
            df['n_pixel'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[1].max())
            df = df[df['most_freq'] > 0]
            
            if len(df) == 0: 
                return
                
            df[['r', 'g', 'b']] = df.most_freq.map(lambda x: pd.Series(LULC_IO_COLORS[x])).apply(pd.Series)
            df['a'] = 255  # Add alpha channel
            df['land_type'] = df.most_freq.map(lambda x: lulc_category_dict.get(x, "Unknown"))
            df['color'] = df.most_freq.map(lambda x: rgb_to_hex(color_map[x]) if x in color_map else "NaN")
            df = df[['hex', 'land_type', 'color', 'r', 'g', 'b', 'a', 'most_freq', 'n_pixel']]
        
            # Print the stats for each tile
            print(df.groupby(['color', 'land_type'])['n_pixel'].sum().sort_values(ascending=False))
            h3_size= res
            df_dem = fused.run("fsh_4bYjNAhhOKALqYMJAP4Sxh", bounds=bounds, h3_size=h3_size)
            df = df.merge(df_dem[['hex', 'metric']], on='hex', how='left')

# Rename the 'metric' column to 'elevation'
            df = df.rename(columns={'metric': 'elevation'})
            return df
            
        except (ValueError, AttributeError) as e:
            print(f"No LULC data available for this region (likely open ocean): {e}")
            return
    else:
        print("Please zoom in more.")