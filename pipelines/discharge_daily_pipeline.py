import os
from typing import Union
from config import PostgresReadWriteConfig
from sqlalchemy import create_engine
import geopandas
import pandas

from pipelines import Transformable, Queryable


class DischargeDailyQueryable(Queryable):

    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/staging.gpkg")

        self.query_str = """
        select encode((date_time::timestamp || site_number || "parameter")::bytea, 'hex') as ckey,
               date_time,
               site_number,
               s.dec_lat_va as latitude,
               s.dec_long_va as longitude,
               "parameter",
               avg_value,
               max_value,
               min_value,
               unit,
               case
                   when value_flag = 'P' then 'Provisional'
                   when value_flag = 'e' then 'Estimated'
                   when value_flag = 'A' then 'Approved'
                   when value_flag = 'A:e' then 'Approved'
                   when value_flag = 'R' then 'Revised'
                   when value_flag = '<' then 'Less than reported'
                   when value_flag = '>' then 'Greater than reported'
                   else null end as value_flag
                from (
                 select
                     dv.datetime as date_time,
                     dv.site_no as site_number,
                     'Discharge' as "parameter",
                     nullif(dv."136080_00060_00003", '') as avg_value,
                     nullif(dv."295487_00060_00001", '') as max_value,
                     nullif(dv."295488_00060_00002", '') as min_value,
                     'cfs' as unit,
                     dv."136080_00060_00003_cd" as value_flag
                 from staging."NWIS_DV_08156675" dv
                 union
                 select
                     dv.datetime as date_time,
                     dv.site_no as site_number,
                     'Discharge' as "parameter",
                     nullif(dv."136084_00060_00003", '') as avg_value,
                     nullif(dv."136082_00060_00001", '') as max_value,
                     nullif(dv."325692_00060_00002", '') as min_value,
                     'cfs' as unit,
                     dv."136084_00060_00003_cd" as value_flag
                 from staging."NWIS_DV_08156800" dv
             ) inner join staging."NWIS_Sites" s on s.site_no = site_number
                order by date_time asc
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        conn_str = PostgresReadWriteConfig().__str__()
        engine = create_engine(conn_str)
        return pandas.read_sql_query(sql=self.query_str, con=engine)


class DischargeDailyTransformable(Transformable):
    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        df['avg_value'] = pandas.to_numeric(df['avg_value'], errors='coerce')
        df['max_value'] = pandas.to_numeric(df['max_value'], errors='coerce')
        df['min_value'] = pandas.to_numeric(df['min_value'], errors='coerce')
        df['date_time'] = pandas.to_datetime(df['date_time'])
        return geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(x=df["longitude"], y=df["latitude"],
                                                                            crs="EPSG:26914"))
