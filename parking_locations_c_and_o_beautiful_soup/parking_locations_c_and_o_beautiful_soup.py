@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely
    import requests
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point
    @fused.cache
    def scrape_and_parse():
    # Required libraries: requests_html, bs4, pandas, geopandas, shapely
# You can install them via pip:
# pip install requests-html bs4 pandas geopandas shapely

        import requests
        from bs4 import BeautifulSoup
        import pandas as pd
        import geopandas as gpd
        from shapely.geometry import Point
        
        # Define the URL and custom headers to avoid a 403 error
        url = "https://candocanal.org/access/"
        headers = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/88.0.4324.190 Safari/537.36")
        }
        
        # Fetch the page
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error: Received status code {response.status_code}")
        
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find and iterate over all tables on the page
        dfs = []
        tables = soup.find_all("table")
        print(f"Found {len(tables)} tables.")
        
        for table in tables:
            rows = table.find_all("tr")
            table_data = []
            
            for row in rows:
                # Extract header cells (<th>) and data cells (<td>)
                cells = row.find_all(["th", "td"])
                row_data = [cell.get_text(strip=True) for cell in cells]
                table_data.append(row_data)
            
            # Skip tables that don't have a header and data rows
            if len(table_data) <= 1:
                continue
            
            # Use the first row as header and the rest as data
            header = table_data[0]
            df = pd.DataFrame(table_data[1:], columns=header)
            dfs.append(df)
        
        # Combine all the table DataFrames into one
        if dfs:
            df_all = pd.concat(dfs, ignore_index=True)
        else:
            raise Exception("No table data found.")
        
        # Function to parse the coordinate string
        def parse_coordinates(coord_str):
            # Remove any extraneous text (like "Google map") and split by comma
            coord_str = coord_str.replace("Google map", "").strip()
            parts = coord_str.split(",")
            if len(parts) < 2:
                return None
            try:
                lat = float(parts[0].strip())
                lon = float(parts[1].strip())
                return lat, lon
            except ValueError:
                return None
        
        # Parse the "Coordinates" column to extract Latitude and Longitude
        df_all["parsed"] = df_all["Coordinates"].apply(parse_coordinates)
        df_all["Latitude"] = df_all["parsed"].apply(lambda x: x[0] if x is not None else None)
        df_all["Longitude"] = df_all["parsed"].apply(lambda x: x[1] if x is not None else None)
        
        # Optionally drop rows where coordinate parsing failed
        df_all = df_all.dropna(subset=["Latitude", "Longitude"])
        
        # Create a geometry column from the Longitude and Latitude (order: lon, lat)
        geometry = [Point(lon, lat) for lon, lat in zip(df_all["Longitude"], df_all["Latitude"])]
        
        # Create the GeoDataFrame and set its CRS to WGS84 (EPSG:4326)
        gdf = gpd.GeoDataFrame(df_all, geometry=geometry, crs="EPSG:4326")
        # print(gdf)
        # Now you have all points from every table in a single GeoDataFrame
        return gdf

    gdf = scrape_and_parse()
    gdf = gdf.rename(columns={'Access Point': 'access_point'})
    # First replace the "0.0 - 1.1" with just "0.0" in the first row
    gdf.loc[0, 'Mile'] = '0.0'

# Convert the Mile column to float type
    import re

# Extract the first float number from the string
    gdf['Mile'] = gdf['Mile'].apply(lambda x: float(re.findall(r'\d+\.?\d*', str(x))[0]))
    print(gdf)
    return gdf
