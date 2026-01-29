import streamlit as st
import folium
from streamlit_folium import st_folium
import geopandas as gpd

# If you're using a real fused object, import it:
# from your_module import fused

def build_map(gdf):
    """Helper function to build a Folium map from a GeoDataFrame."""
    m = folium.Map(location=[37.7749, -122.4194], zoom_start=13)
    if not gdf.empty:
        folium.GeoJson(
            gdf,
            name="Result Data"
        ).add_to(m)
    folium.LayerControl().add_to(m)
    return m

def main():
    st.set_page_config(page_title="Single Map with Costing & Time Step", layout="wide")
    st.title("One Map: Click → fused.run() → Updated Data & GeoJSON Download")

    # --- Sidebar for costing & time_step (with defaults) ---
    st.sidebar.header("Routing Options")
    costing = st.sidebar.selectbox(
        "Select Costing",
        ["auto", "pedestrian", "bicycle", "truck", "bus", "motor_scooter"],
        index=1  # Default: "pedestrian"
    )
    time_step = st.sidebar.selectbox(
        "Select Time Step (minutes)",
        [1, 5, 10, 15, 20, 30, 45, 60],
        index=2  # Default: 10
    )

    # --- Initialize GeoDataFrame in session state ---
    if "gdf" not in st.session_state:
        st.session_state.gdf = gpd.GeoDataFrame()

    # Create an empty container where we'll render (and re-render) our map
    map_container = st.empty()

    # 1) Build & show the map with the current GDF (if any)
    initial_map = build_map(st.session_state.gdf)

    # Display the map inside the container
    with map_container:
        map_return = st_folium(initial_map, width=700, height=500)

    # 2) If the user has clicked, call fused.run(...) and rebuild the map
    if map_return and "last_clicked" in map_return and map_return["last_clicked"]:
        lat = map_return["last_clicked"]["lat"]
        lng = map_return["last_clicked"]["lng"]
        st.success(f"Clicked: lat={lat}, lng={lng}")

        with st.spinner("Processing..."):
            # EXACT line where we call fused.run() with costing and time_step
            st.session_state.gdf = fused.run(
                'fsh_3dvukY57Cab5mc5l1FPtVT',       # Use your actual token or logic here
                lat=lat,
                lng=lng,
                costing=costing,
                time_step=time_step
            )
        st.success("Data processed! Updating the map...")

        # Build a new map with the updated data
        updated_map = build_map(st.session_state.gdf)

        # Re-render the new map in the same container
        with map_container:
            map_return = st_folium(updated_map, width=700, height=500)

    # 3) Display the GDF table & download button if we have data
    if not st.session_state.gdf.empty:
        st.subheader("Resulting Data:")
        st.write(st.session_state.gdf)

        # Optional: download as GeoJSON
        geojson_str = st.session_state.gdf.to_json()
        st.download_button(
            label="Download as GeoJSON",
            data=geojson_str,
            file_name="results.geojson",
            mime="application/json"
        )

if __name__ == "__main__":
    main()
