import geojson
from shapely import Point
import rioxarray as rxr
import shapely
import geopandas as gpd
from utils import reproject_shapely
  

def open_raster(filename):
    """
    # TODO
    
    Attributes
    ----------
    # TODO
    
    Returns
    -------
    # TODO
    """
    
    dataarray = rxr.open_rasterio(filename)
    return dataarray


def crop_raster(raster,cropping_polygon):
    """
    # TODO
    
    Attributes
    ----------
    # TODO
    
    Returns
    -------
    # TODO
    """

    cropping_geometry = [geojson.loads(shapely.to_geojson(cropping_polygon))]
    cropped = raster.rio.clip(geometries=cropping_geometry, from_disk=True)
    cropped = cropped.loc[1, :, :].to_pandas()
    
    return cropped


def vectorize_raster_grid(raster, cropping_polygon, poly_crs=4326, raster_crs="ESRI:54009", grid_resolution=100,value_column_name='value'):
    """
    # TODO
    
    Attributes
    ----------
    # TODO
    
    Returns
    -------
    # TODO
    """
    
    cropping_polygon = reproject_shapely(cropping_polygon,poly_crs,raster_crs)
    raster_cropped = crop_raster(raster,cropping_polygon)

    res = []
    for x in raster_cropped.columns:
        for y in raster_cropped.index:
            res.append([Point(x + (grid_resolution / 2), y - (grid_resolution / 2)), raster_cropped.loc[y, x]])

    gdf = gpd.GeoDataFrame(res,columns=['geometry',value_column_name],crs=raster_crs)
    gdf['geometry'] = gdf.buffer(grid_resolution / 2, cap_style=3)

    return gdf


def project_grid_values(gdf, grid, gdf_id_column,grid_value_column='value'):
    """
    # TODO

    Attributes
    ----------
    # TODO
    
    Returns
    -------
    # TODO
    """
    
    pop_cell_area = grid[:1].area.max()
    
    grid_crs = grid.crs
    gdf_grid_overlay = gdf[[gdf_id_column, "geometry"]].to_crs(grid_crs).overlay(grid, how="intersection")
    
    gdf_grid_overlay["area"] = gdf_grid_overlay.area
    gdf_grid_overlay["area"] = gdf_grid_overlay["area"] / pop_cell_area
    gdf_grid_overlay[grid_value_column] = gdf_grid_overlay[grid_value_column] * gdf_grid_overlay["area"]
    
    res = gdf_grid_overlay.groupby(gdf_id_column)[grid_value_column].sum().reset_index()
    gdf = gdf.merge(res, how="left")

    return gdf