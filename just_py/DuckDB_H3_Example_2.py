@fused.udf
def udf(bounds: fused.types.Bounds = [-74.033,40.648,-73.788,40.846], resolution: int = 9, min_count: int = 10):
    import shapely
    import geopandas as gpd
    import pandas as pd
    
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    con = common.duckdb_connect()

    @fused.cache
    def read_data(url, resolution, min_count):
        df = con.sql("""
        SELECT h3_h3_to_string(h3_latlng_to_cell(pickup_latitude, pickup_longitude, $resolution)) cell_id,
               h3_cell_to_boundary_wkt(cell_id) boundary,
               count(1) cnt
        FROM read_parquet($url) 
        GROUP BY cell_id
        HAVING cnt>$min_count
        """, params={'url': url, 'resolution': resolution, 'min_count': min_count}).df()
        return df

    df = read_data('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2010-01.parquet', resolution, min_count)
    
    # Create geodataframe
    gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    
    # Convert to list of features for deck.gl
    features = []
    for _, row in gdf.iterrows():
        coords = list(row.geometry.exterior.coords)
        features.append({
            "type": "Feature",
            "properties": {
                "cell_id": row.cell_id,
                "cnt": row.cnt
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [coords]
            }
        })
    
    # Create HTML with deck.gl dashboard
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>NYC Taxi Pickups Dashboard</title>
        <script src="https://unpkg.com/deck.gl@8.8.0/dist.min.js"></script>
        <script src="https://unpkg.com/@deck.gl/geo-layers@8.8.0/dist.min.js"></script>
        <script src="https://unpkg.com/@deck.gl/aggregation-layers@8.8.0/dist.min.js"></script>
        <style>
            body {{ margin: 0; font-family: Arial, sans-serif; }}
            #map {{ width: 100vw; height: 100vh; }}
            #controls {{
                position: absolute;
                top: 20px;
                left: 20px;
                background: rgba(255, 255, 255, 0.9);
                padding: 15px;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.2);
                max-width: 300px;
            }}
            #legend {{
                position: absolute;
                bottom: 20px;
                right: 20px;
                background: rgba(255, 255, 255, 0.9);
                padding: 15px;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            }}
            .legend-item {{
                display: flex;
                align-items: center;
                margin: 5px 0;
            }}
            .legend-color {{
                width: 20px;
                height: 20px;
                margin-right: 10px;
                border-radius: 3px;
            }}
            h3 {{ margin: 0 0 10px 0; }}
            .stat {{ margin: 5px 0; font-size: 14px; }}
        </style>
    </head>
    <body>
        <div id="map"></div>
        <div id="controls">
            <h3>NYC Taxi Pickups</h3>
            <div class="stat">Resolution: {resolution}</div>
            <div class="stat">Min Count: {min_count}</div>
            <div class="stat">Total Trips: {df.cnt.sum():,}</div>
            <div class="stat">Active Cells: {len(df)}</div>
        </div>
        <div id="legend">
            <h3>Pickup Density</h3>
            <div class="legend-item">
                <div class="legend-color" style="background: rgb(255, 50, 50)"></div>
                <span>High (>50k trips)</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: rgb(255, 150, 50)"></div>
                <span>Medium (10k-50k)</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: rgb(50, 150, 255)"></div>
                <span>Low (<10k)</span>
            </div>
        </div>

        <script>
            const data = {features};
            
            // Calculate color based on count using the same formula as original
            function getColor(count) {{
                const r = Math.min(255, Math.floor(count/3 + 200));
                const g = Math.min(255, Math.floor(count/5 + 50));
                const b = Math.min(255, Math.floor(count/20));
                return [r, g, b, 200];
            }}
            
            // Calculate elevation based on count
            function getElevation(count) {{
                return Math.log(count) * 100;
            }}

            const deckgl = new deck.DeckGL({
                container: 'map',
                mapStyle: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
                initialViewState: {
                    longitude: -73.9,
                    latitude: 40.75,
                    zoom: 15,
                    pitch: 45,
                    bearing: 0
                },
                controller: true,
                layers: [
                    new deck.H3HexagonLayer({
                        id: 'h3-hexagon-layer',
                        data: data,
                        pickable: true,
                        wireframe: false,
                        filled: true,
                        extruded: true,
                        elevationScale: 1,
                        getHexagon: d => d.properties.cell_id,
                        getFillColor: d => getColor(d.properties.cnt),
                        getElevation: d => getElevation(d.properties.cnt),
                        opacity: 0.8
                    })
                ],
                getTooltip: ({object}) => {{
                    if (object) {{
                        return {{
                            html: `
                                <div><strong>Cell ID:</strong> ${{object.properties.cell_id}}</div>
                                <div><strong>Trips:</strong> ${{object.properties.cnt.toLocaleString()}}</div>
                            `
                        }};
                    }}
                    return null;
                }}
            });
        </script>
    </body>
    </html>
    """
    
    return common.html_to_obj(html_content)