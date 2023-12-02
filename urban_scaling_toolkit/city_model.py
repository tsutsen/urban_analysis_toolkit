import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.ops import polygonize
from shapely import Polygon,MultiPolygon
import utils
from utils import verbose_print,get_attribute_from_largest_intersection
from clusterizer import compress_services,get_cluster_hulls
import metrics
import raster_handler

    
class CityModel:
        
    def __init__(self, territory, roads=None, railways=None, water=None, verbose=True):
        
        self.verbose = verbose
        
        self.water = None
        if water is not None:
            territory = territory.reset_index().overlay(water[water.geom_type != 'LineString'].reset_index(),how='difference') # cut water polygons from territory
            self.water = water.geometry if type(water) == gpd.GeoDataFrame else water
            self.water = self.water.map(lambda x: x.boundary if x.geom_type in ['Polygon','MultiPolygon'] else x).set_crs(4326)
        
        self.territory = territory.unary_union
        
        self.local_crs = utils.get_projected_crs(self.territory)

        self.roads = roads.geometry if type(roads) == gpd.GeoDataFrame else roads
        self.railways = railways.geometry if type(railways) == gpd.GeoDataFrame else railways

        self.blocks = None
        self.services = None
        self.cluster_polygons = None
        self.cluster_info = None
                
            
    def generate_blocks(self, min_block_width=None):
        """
        # TODO
        
        Attributes
        ----------
        # TODO
        
        Returns
        -------
        # TODO
        """
               
        utils.verbose_print("GENERATING BLOCKS", self.verbose)
        
        # create a GeoDataFrame with barriers
        barriers = gpd.GeoDataFrame(geometry=pd.concat([self.roads, self.water, self.railways]),crs=4326).reset_index(drop=True)
        barriers = barriers.explode(index_parts=True).reset_index(drop=True).geometry
        
        # transform enclosed barriers to polygons 
        utils.verbose_print("Setting up enclosures...", self.verbose)
        blocks = self._get_enclosures(barriers,self.territory)

        # fill everything within blocks' boundaries
        utils.verbose_print("Filling holes...", self.verbose)
        blocks = utils.fill_holes(blocks)

        # cleanup after filling holes
        utils.verbose_print("Dropping overlapping blocks...", self.verbose)
        blocks = utils.drop_contained_geometries(blocks)
        blocks = blocks.explode(index_parts=False).reset_index(drop=True)

        blocks = blocks.rename(columns={"index": "block_id"})

        # apply negative and positive buffers consecutively to remove small blocks
        # and divide the blocks on bottlenecks
        if min_block_width is not None:
            utils.verbose_print("Filtering bottlenecks and small blocks...", self.verbose)
            blocks = utils.filter_bottlenecks(blocks, self.local_crs, min_block_width)
            blocks = self._reindex_blocks(blocks)

        # calculate blocks' area using local projected CRS
        utils.verbose_print("Calculating blocks area...", self.verbose)
        blocks["area"] = blocks.to_crs(self.local_crs).area
        blocks = blocks[blocks["area"] > 1]

        # fix blocks' indices
        blocks = self._reindex_blocks(blocks)

        self.blocks = blocks
        utils.verbose_print("Blocks generated.\n", self.verbose)
   
    def set_services(self,services):
        self.services = services
        self._link_services_to_blocks()
    
    def cluster_blocks(self, clustering_distance=1200, method="average", max_number_of_services=10000):
        """
        # TODO
        
        Attributes
        ----------
        # TODO
        
        Returns
        -------
        # TODO
        """
             
        verbose_print("CLUSTERING BLOCKS", self.verbose)
        
        _services = self.services.copy()

        # if there are more than X services, perform service clustering 
        if len(_services) > max_number_of_services:
            verbose_print("Too many services. Compressing...", self.verbose)
            _services = compress_services(_services, n_clusters=max_number_of_services)
            verbose_print("Services compressed. Proceeding with clustering...", self.verbose)

        # cluster services, create convex hulls from cluster points and assign clusters to blocks
        self.cluster_polygons = get_cluster_hulls(
            _services.to_crs(self.local_crs),
            distance_limit=clustering_distance,
            link=method)
        
        self.blocks = get_attribute_from_largest_intersection(
            self.blocks, self.cluster_polygons, 
            attribute_column="cluster",
            df_id_column="block_id",
            projected_crs=self.local_crs)
        
        # if cluster polygon occupies less than X% of block's area, unassign cluster from this block
        self.blocks.loc[self.blocks["intersection_area"] < 0.4, "cluster"] = np.nan
        self.blocks = self.blocks.drop('intersection_area',axis=1)
        
        self._link_services_to_blocks()
        verbose_print("Blocks clustered.\n", self.verbose)
             
                
    def evaluate_centrality(self):
        """
        # TODO
        
        Attributes
        ----------
        # TODO
        
        Returns
        -------
        # TODO
        """
        
        self.blocks = metrics.evaluate_centrality(self.blocks,self.services)
    
    
    def populate_blocks(self,population_raster_path="GHS_POP_E2020.tif"):
        """
        # TODO
        
        Attributes
        ----------
        # TODO
        
        Returns
        -------
        # TODO
        """
        
        population_raster = raster_handler.open_raster(population_raster_path)
        
        population_grid = raster_handler.vectorize_raster_grid(population_raster,self.territory,value_column_name='population')
        population_grid = population_grid.query('population>=0')
        
        self.blocks = raster_handler.project_grid_values(self.blocks,population_grid,'block_id','population')
        self.blocks['population'] = self.blocks['population'].fillna(0).round().astype(int)
        
        
    def aggregate_cluster_info(self):
        """
        # TODO
        
        Attributes
        ----------
        # TODO
        
        Returns
        -------
        # TODO
        """
        
        self.blocks['area'] = self.blocks.to_crs(self.local_crs).area
        
        cluster_info = (
            self.blocks.groupby("cluster").agg(
                {
                    "centrality_bin": "first",
                    "centrality": "first",
                    "diversity": "first",
                    "services_total": "first",
                    "area": "sum",
                    "population": "sum",
                    "geometry": list,
                }
            ).reset_index())
        
        cluster_info["geometry"] = cluster_info["geometry"].map(
            lambda x: gpd.GeoDataFrame(geometry=x).unary_union)
        cluster_info = gpd.GeoDataFrame(cluster_info,crs=4326)
        
        service_stats_tags = self.services.groupby("cluster")["tags"].apply("sum")
        service_stats_tags = service_stats_tags.map(
            lambda x: [*pd.Series(x).value_counts().nlargest(3).keys()]).reset_index()
        
        service_stats_categories = self.services.groupby("cluster")["category"].apply(list)
        service_stats_categories = service_stats_categories.map(
            lambda x: [*pd.Series(x).value_counts().nlargest(3).keys()]).reset_index()

        service_category_counts = self.services.groupby(
            ["cluster", "category"])["geometry"].count().unstack().reset_index()
        
        cluster_info = cluster_info.merge(service_category_counts, how="left")
        self.cluster_info = cluster_info
        
        
    def _link_services_to_blocks(self):
        """
        # TODO
        
        Attributes
        ----------
        # TODO
        
        Returns
        -------
        # TODO
        """
           
        blocks_columns = ["block_id", "geometry"]
        if "cluster" in self.blocks.columns:
            blocks_columns.append("cluster")

        # drop block_id and cluster columns in services
        self.services = self.services.drop(['block_id','cluster'], axis=1, errors='ignore')

        # add block_id and cluster from the nearest block to the services
        self.services = self.services.to_crs(self.local_crs)
        self.services = self.services.sjoin_nearest(self.blocks.to_crs(self.local_crs)[blocks_columns], 
                                                    how="left", max_distance=200).to_crs(4326)
        
        # drop services that are not assigned to any block
        self.services = self.services.dropna(subset="block_id")
        
        # clean up indices in services GeoDataFrame
        self.services = self.services.drop("index_right", axis=1)
        self.services["block_id"] = self.services["block_id"].astype(int)
        self.services = self.services.reset_index(drop=True)
        
        
    @staticmethod
    def _get_enclosures(barriers,limit):
        """
        # TODO
        
        Attributes
        ----------
        # TODO
        
        Returns
        -------
        # TODO
        """
        
        if type(limit) in [Polygon,MultiPolygon]:
            limit = gpd.GeoSeries(limit,crs=4326)
        
        barriers = pd.concat([barriers,limit.boundary]).reset_index(drop=True)

        unioned = barriers.unary_union
        polygons = polygonize(unioned)
        enclosures = gpd.GeoSeries(list(polygons), crs=barriers.crs)
        _, enclosure_idxs = enclosures.representative_point().sindex.query(limit.geometry, predicate="contains")
        enclosures = enclosures.iloc[np.unique(enclosure_idxs)]
        enclosures = enclosures.rename('geometry').reset_index()
        
        return enclosures
          
            
    @staticmethod
    def _reindex_blocks(blocks):
        """
        # TODO
        
        Attributes
        ----------
        # TODO
        
        Returns
        -------
        # TODO
        """
        
        if 'block_id' in blocks.columns:
            blocks = blocks.drop('block_id',axis=1).reset_index().rename(columns={'index':'block_id'})
        return blocks
    
    
    def explore(self,column=None,cmap='Blues',attribute='blocks',tiles='CartoDB Positron'):
        """
        # TODO
        
        Attributes
        ----------
        # TODO
        
        Returns
        -------
        # TODO
        """
        
        if attribute=='blocks':
            m = self.blocks.explore(column=column,tiles=tiles)
        return m