import dask_geopandas
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely import MultiPoint
from sklearn.cluster import AgglomerativeClustering, MiniBatchKMeans


def get_distance_matrix(geom): 
    """
    # TODO
    
    Attributes
    ----------
    # TODO
    
    Returns
    -------
    # TODO
    """
    
    ddf = dask_geopandas.from_geopandas(geom, npartitions=5)
    meta_df = pd.DataFrame(np.nan, index=geom.index, columns=geom.index)
    distance_matrix = ddf["geometry"].apply(lambda geom_2: geom.distance(geom_2), meta=meta_df)

    return distance_matrix


def get_cluster_hulls(services, distance_limit=1000, link="average"):
    """
    # TODO
    
    Attributes
    ----------
    # TODO
    
    services: gpd.GeoDataFrame
        services in a projected metric CRS
    
    Returns
    -------
    # TODO
    """
    
    distance_matrix = get_distance_matrix(services)
    
    services = services.to_crs(4326)
    services["cluster"] = AgglomerativeClustering(
        n_clusters=None,metric="precomputed",distance_threshold=distance_limit,linkage=link).fit_predict(distance_matrix)

    services_per_cluster = services.groupby(["cluster"])["geometry"].count()
    
    hulls = []
    for cluster in services_per_cluster[services_per_cluster > 4].index:
        hulls.append([cluster,services[services["cluster"] == cluster]["geometry"].unary_union.convex_hull])

    cluster_polygons = gpd.GeoDataFrame(hulls)
    cluster_polygons.columns = ["cluster", "geometry"]
    cluster_polygons = cluster_polygons.set_geometry("geometry").set_crs(4326)
    cluster_polygons = cluster_polygons[cluster_polygons.type == "Polygon"]

    return cluster_polygons

    
def compress_services(df, n_clusters=10000):
    """
    # TODO
    
    Attributes
    ----------
    # TODO
    
    Returns
    -------
    # TODO
    """
      
    X = (df["geometry"].map(lambda x: [x.x, x.y])).tolist()

    clustering_services = MiniBatchKMeans(n_clusters=n_clusters, random_state=42, n_init="auto").fit(X)

    df["cluster_temp"] = clustering_services.labels_

    df_clustered = gpd.GeoSeries(
        df.groupby(["cluster_temp"])["geometry"].apply(list).apply(MultiPoint)).reset_index().set_crs(4326)
    
    tags_df = df.groupby(["cluster_temp"]).agg({"tags": "sum"}).reset_index()
    df_clustered = df_clustered.merge(tags_df)
    df_clustered["geometry"] = df_clustered.to_crs(3857).centroid.to_crs(4326)
    df_clustered = df_clustered.drop("cluster_temp", axis=1)

    return df_clustered