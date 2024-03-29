import os
from typing import Union

import geopandas
import pandas
import shapely
from shapely.wkt import loads

from pipelines import Transformable, Queryable

geopandas.options.io_engine = "pyogrio"


class HydrographyQueryable(Queryable):

    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/staging.gpkg")

        self.query_str = """
        SELECT 
         OBJECTID,
         Shape as geometry, 
         gnis_name as name, 
         SHAPE_Leng as length_geom, 
         lengthkm as length_km, 
         fcode as feature_code 
         FROM NHD_Flowline_TC
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        return geopandas.read_file(filename=self.db_path, sql=self.query_str, crs="EPSG:26914")


class HydrographyTransformable(Transformable):

    def __init__(self, boundary_geometry: shapely.Geometry):
        self.boundary_geometry = boundary_geometry

    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        flow_lines = geopandas.GeoDataFrame.copy(df)
        return flow_lines[flow_lines.intersects(self.boundary_geometry)]
