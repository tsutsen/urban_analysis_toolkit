import geopandas as gpd
import pandas as pd
import momepy as mm
import numpy as np

    
def evaluate_centrality(blocks,services):
    """
    # TODO
    
    Attributes
    ----------
    # TODO
    
    Returns
    -------
    # TODO
    """    
    
    # drop columns related to centrality if there are any
    blocks = blocks.drop(['diversity','centrality','centrality_bin','services_total'],axis=1,errors='ignore')
    
    # collect all service tags per cluster
    services_groupped = services.groupby('cluster_id').agg({"tags": "sum"})["tags"].apply(list)
    
    # calculate diversity per cluster
    diversity = pd.DataFrame(columns=['cluster_id', "diversity"])
    for i, x in services_groupped.items():
        diversity.loc[len(diversity)] = [i,1 - mm.simpson_diversity(pd.Series(x), categorical=True)]

    # count services per cluster
    services_total = services.groupby('cluster_id')["geometry"].count().rename("services_total").reset_index()
    
    # merge diversity info and cluster count with blocks dataframe
    blocks = blocks.merge(diversity, how="left",on='cluster_id')
    blocks = blocks.merge(services_total, how="left",on='cluster_id')

    #self.blocks = self.blocks.merge(self.blocks.groupby('cluster_id')["area"].sum().rename("cluster_area").reset_index(),how="left")
    
    # max-normalize diversity and service count 
    diversity_normalized = blocks["diversity"] / blocks["diversity"].max()
    services_total_normalized = blocks["services_total"] / blocks["services_total"].max()
    
    # compute centrality
    blocks["centrality"] = diversity_normalized + services_total_normalized
    blocks["centrality"] = blocks["centrality"].fillna(0)

    # max-normalize centrality
    blocks["centrality"] = blocks["centrality"] / blocks["centrality"].max()
    
    # map centrality to intervals from 0 to 10
    intervals = np.arange(-10, 111, 10) / 100
    intervals_levels = {y: x for x, y in zip(np.arange(len(intervals)),pd.cut(np.asarray(blocks["centrality"]), intervals).categories)}
    blocks["centrality_bin"] = pd.cut(np.asarray(blocks["centrality"]), intervals)
    blocks["centrality_bin"] = blocks["centrality_bin"].replace(intervals_levels)
    blocks["centrality_bin"] = blocks["centrality_bin"].fillna(0).astype(int)
    
    return blocks