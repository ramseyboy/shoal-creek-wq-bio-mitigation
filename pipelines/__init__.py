from pathlib import Path
from typing import Union
from enum import Enum
import geopandas
import pandas
from typing import TypeVar
from abc import ABC
from osgeo import ogr
import tempfile, subprocess, os

from sqlalchemy import create_engine

TPipeline = TypeVar("TPipeline", bound="Pipeline")


class Loadable(ABC):

    def __init__(self, uri: str):
        self.uri = uri

    def load(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        pass


class Queryable(ABC):

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        pass


class Transformable(ABC):

    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        pass


class Pipeline:

    def __init__(self):
        self.df = geopandas.GeoDataFrame()

    def load(self, load: Loadable) -> TPipeline:
        self.df = load.load()
        return self

    def query(self, queryable: Queryable) -> TPipeline:
        self.df = queryable.query()
        return self

    def transform(self, transformable: Transformable) -> TPipeline:
        self.df = transformable.transform(self.df)
        return self

    def dataframe(self) -> geopandas.GeoDataFrame:
        return self.df

    def export_geopackage(self, geopackage_path: str, layer_name: str):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_gpkg_path = os.path.join(temp_dir, 'temp.gpkg')
            self.df.to_file(temp_gpkg_path, layer=layer_name, driver="GPKG", mode='w')
            append_cmd = f'ogr2ogr -f "GPKG" {geopackage_path} "{temp_gpkg_path}" -nln {layer_name} -progress -update -overwrite'
            subprocess.run(append_cmd, shell=True)

    def export_parquet(self, layer_name: str):
        Path(f"data/{layer_name}").mkdir(parents=True, exist_ok=True)
        self.df.to_parquet(f"data/{layer_name}/{layer_name}.parquet")

    def export_postgis(self, conn_str: str, layer_name: str):
        engine = create_engine(conn_str)
        self.df.to_postgis(layer_name, con=engine, if_exists='replace')

