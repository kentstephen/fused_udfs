@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int=10):
    import geopandas as gpd
    import shapely
    import networkx as nx
    import osmnx as ox
    import networkx as nx
    import matplotlib.pyplot as plt
    
    # Define the place
    place_name = "Boulder, Colorado, USA"
    
    # Retrieve only pedestrian-related ways
    custom_filter = '["highway"~"footway|path|pedestrian|crossing|sidewalk"]'
    
    # Load pedestrian network
    G = ox.graph_from_place(place_name, network_type="walk", custom_filter=custom_filter)
    
    # Convert to an undirected graph for centrality analysis
    G_undirected = G.to_undirected()
    # Compute closeness centrality
    closeness_centrality = nx.closeness_centrality(G_undirected)
    
    # Assign values to the graph nodes
    nx.set_node_attributes(G_undirected, closeness_centrality, "closeness")
    
    # Convert to a GeoDataFrame
    gdf_nodes = ox.graph_to_gdfs(G_undirected, edges=False)
    
    # Add closeness values
    gdf_nodes["closeness"] = gdf_nodes.index.map(closeness_centrality)
    import h3
    import pandas as pd
    from shapely.geometry import Point
    
      
    
    # Ensure nodes have latitude and longitude
    gdf_nodes["lat"] = gdf_nodes.geometry.y
    gdf_nodes["lng"] = gdf_nodes.geometry.x
    
    # Assign each node to an H3 hexagon
    gdf_nodes["h3"] = gdf_nodes.apply(lambda row: h3.str_to_int(h3.latlng_to_cell(row["lat"], row["lng"], resolution)), axis=1)
    
    # Aggregate closeness centrality by H3 cell
    df_h3 = gdf_nodes.groupby("h3")["closeness"].mean().reset_index()
    df_h3.columns = ["h3", "avg_closeness"]
    df_h3["norm_closeness"] = 255 * (df_h3["avg_closeness"] - df_h3["avg_closeness"].min()) / (
    df_h3["avg_closeness"].max() - df_h3["avg_closeness"].min()
)

    # Ensure H3 indexes are strings
    df_h3["h3"] = df_h3["h3"].astype(int)
    
    # Prepare DataFrame for Pydeck
    df = pd.DataFrame({
        "hex": df_h3["h3"],
        "value": df_h3["avg_closeness"],
        "norm_value": df_h3["norm_closeness"]
    })
    # print(df)
    return df

