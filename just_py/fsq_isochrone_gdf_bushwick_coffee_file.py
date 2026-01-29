@fused.udf
def udf(n: int=10):
    @fused.cache
    def get_iso_gdf():
        import shapely
        import geopandas as gpd
        import pandas as pd
        # Greater Bushiwck
        bbox = gpd.GeoDataFrame(
            geometry=[shapely.box(-73.966036,40.666722,-73.875359,40.726179)], 
            crs=4326
        )
       
        def get_coffee(bbox):
            # Read the GeoJSON file from NYC Open Data
            gdf_tract = gpd.read_file('https://data.cityofnewyork.us/api/geospatial/j2bc-fus8?method=export&format=GeoJSON')
            
            # Filter for rows containing 'Bushwick' in ntaname and dissolve, keeping the name
            gdf_tract = gdf_tract[gdf_tract['ntaname'].str.contains('Bushwick', na=False)].dissolve(by='ntaname').reset_index()
            df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
            if len(df) < 1:
                return
            df = df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
            # Apply the mask
            df = gpd.overlay(df, gdf_tract, how='intersection')
        df = = get_coffee(bbox=bbox)
        def batch_isochrone_analysis(df):
            import time
            import geopandas as gpd
            all_isochrones = []
            
            for point in df.geometry:
                try:
                    gdf = fused.utils.Get_Isochrone.get_isochrone(
                        lat=point.y,
                        lng=point.x,
                        costing="pedestrian",
                        time_steps=[5]
                    )
                    all_isochrones.append(gdf)
                    # time.sleep(1)
                except Exception as e:
                    print(f"Error processing point ({point.x}, {point.y}): {str(e)}")
                    # Create empty GeoDataFrame with same structure
                    empty_gdf = gpd.GeoDataFrame(
                        geometry=[], 
                        crs="EPSG:4326"
                    )
                    all_isochrones.append(empty_gdf)
                    continue
        
            return pd.concat(all_isochrones) if all_isochrones else gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        gdf = batch_isochrone_analysis(df=df)
        return gdf