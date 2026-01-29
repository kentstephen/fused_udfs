@fused.udf 
def udf(
    # bbox: fused.types.TileGDF = None,
    release: str = "2024-12-18-0",
    theme: str = None,
    overture_type: str = None, 
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None
):
    import shapely
    import geopandas as gpd
    from utils import get_overture
    # df = fused.run("fsh_4lOaYrPBR5WAbtZsdGSX8M")
    bbox = gpd.GeoDataFrame(
    geometry=[shapely.box(-75.35,42.42,-66.22,47.77)], #new england
    crs=4326
)
    
    gdf = get_overture(
        bbox=bbox,
        release=release,
        theme=theme,
        overture_type=overture_type,
        use_columns=use_columns,
        num_parts=num_parts,
        min_zoom=min_zoom,
        polygon=polygon,
        point_convert=point_convert
    )
    if len(gdf) < 1:
        return
    gdf =  gdf[gdf["subtype"]=='region']
    df = fused.run("fsh_4lOaYrPBR5WAbtZsdGSX8M")
    def get_boundaries(df):
        con = fused.utils.common.duckdb_connect()
        query="""select hex, h3_cell_to_boundary_wkt(hex) as boundary from df"""
        df = con.sql(query).df()
        return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    h3_gdf = get_boundaries(df)
    gdf_joined = gdf.sjoin(h3_gdf)
    return gdf_joined