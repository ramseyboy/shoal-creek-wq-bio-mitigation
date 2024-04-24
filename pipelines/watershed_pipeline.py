import os
from typing import Union

import geopandas
import pandas
from shapely.wkt import loads
from sqlalchemy import create_engine

from config import PostgresReadWriteConfig
from pipelines import Transformable, Queryable

geopandas.options.io_engine = "pyogrio"


class WatershedQueryable(Queryable):

    def __init__(self):
        self.conn_str = PostgresReadWriteConfig().__str__()
        self.engine = create_engine(self.conn_str)

        self.query_str = """
        SELECT * FROM public.watershed w
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        return geopandas.read_postgis(
            sql=self.query_str,
            con=self.engine, geom_col='geometry', crs="EPSG:26914")


class WatershedTransformable(Transformable):
    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        return geopandas.GeoDataFrame(df)