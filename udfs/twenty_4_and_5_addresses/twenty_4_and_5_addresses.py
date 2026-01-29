@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely

    def get_overture(bbox):
        gdf = fused.utils.Overture_Maps_Example.get_overture(overture_type='address',bbox=bbox, min_zoom=0)
        if len(gdf) < 1:
            return
        return gdf
    gdf = get_overture(bbox)
    if gdf is None or gdf.empty:
        return
    @fused.cache
    def mask():
        return gpd.read_file('https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_0_countries.geojson').query("ADMIN == 'France'")
    # mask = mask()
    # gdf = gpd.overlay(gdf, mask, how='intersection')
    # gdf = gdf[gdf["number"].str.contains("2025", case=False, na=False)]
    # print(gdf.columns)
    @fused.cache
    def nye(gdf):
        colors = {
        '25': (255, 215, 0),  # Gold
        '24': (192, 192, 192),  # Silver
        # bright blue
    }
        # Initialize r, g, b columns
        gdf['r'] = None
        gdf['g'] = None
        gdf['b'] = None
        
        # Assign colors based on street name
        for keyword, color in colors.items():
            mask = gdf['number'].str.contains(keyword, case=False, na=False)
            gdf.loc[mask, 'r'] = color[0]
            gdf.loc[mask, 'g'] = color[1]
            gdf.loc[mask, 'b'] = color[2]
        
        # Drop rows without any color assignment (optional)
        gdf = gdf.dropna(subset=['r', 'g', 'b'], how='all')
        
        # Convert r, g, b to integers
        gdf[['r', 'g', 'b']] = gdf[['r', 'g', 'b']].astype(int)
        return gdf
    df = nye(gdf)
    return df
