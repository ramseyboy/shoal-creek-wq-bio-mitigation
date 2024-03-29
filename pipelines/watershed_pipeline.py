import os
from typing import Union

import geopandas
import pandas
from shapely.wkt import loads

from pipelines import Transformable, Queryable

geopandas.options.io_engine = "pyogrio"


class WatershedQueryable(Queryable):

    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/staging.gpkg")

        self.query_str = """
        SELECT 
             OBJECTID, 
             WATERSHED_FULL_NAME as watershed_name, 
             the_geom as geometry, 
             SHAPE_Area as area, 
             SHAPE_Length as length, 
             RECEIVING_BASIN as receiving_basin, 
             RECEIVING_WATERS as receiving_waters, 
             WATERSHED_ID as watershed_id 
             FROM WBD_COA 
             where watershed_name = 'Shoal Creek'
             order by length desc
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        return geopandas.read_file(filename=self.db_path, sql=self.query_str)


class WatershedTransformable(Transformable):
    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        watersheds = geopandas.GeoDataFrame(df)
        watersheds.geometry = df['geometry'].apply(loads)
        watersheds = watersheds.set_crs(crs="EPSG:4326")
        return watersheds.to_crs(crs="EPSG:26914")