import os
from typing import Union

import geopandas
import pandas

from pipelines import Transformable, Queryable


class DischargeQueryable(Queryable):

    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/staging.gpkg")

        self.query_str = """
select hex(CAST(strftime('%s', date_time) AS INT) || site_number || parameter) as ckey,
       (substr(date_time, 0, 11) || 'T' || substr(date_time, 12, 6)) as date_time,
       site_number,
       s.dec_lat_va as latitude,
       s.dec_long_va as longitude,
       parameter,
       primary_value,
       secondary_value,
       unit,
       case
           when primary_value_flag = 'P' then 'Provisional'
           when primary_value_flag = 'e' then 'Estimated'
           when primary_value_flag = 'A' then 'Approved'
           when primary_value_flag = 'R' then 'Revised'
           when primary_value_flag = '<' then 'Less than reported'
           when primary_value_flag = '>' then 'Greater than reported'
           else null end as primary_value_flag,
       case
           when secondary_value_flag = 'P' then 'Provisional'
           when secondary_value_flag = 'e' then 'Estimated'
           when secondary_value_flag = 'A' then 'Approved'
           when secondary_value_flag = 'R' then 'Revised'
           when secondary_value_flag = '<' then 'Less than reported'
           when secondary_value_flag = '>' then 'Greater than reported'
           else null end as secondary_value_flag
        from (
         select
             iv.datetime as date_time,
             iv.site_no as site_number,
             'Discharge' as parameter,
             nullif(iv."141286_00060", '') as primary_value,
             nullif(iv."227895_00060", '') as secondary_value,
             'cfs' as unit,
             iv."141286_00060_cd" as primary_value_flag,
             iv."227895_00060_cd" as secondary_value_flag
         from NWIS_IV_08156800 iv
         union
         select
             iv.datetime as date_time,
             iv.site_no as site_number,
             'Gage height' as parameter,
             nullif(iv."141287_00065", '') as primary_value,
             nullif(iv."224497_00065", '') as secondary_value,
             'ft' as unit,
             iv."141287_00065_cd" as primary_value_flag,
             iv."224497_00065_cd" as secondary_value_flag
         from NWIS_IV_08156800 iv
         union
         select
             iv.datetime as date_time,
             iv.site_no as site_number,
             'Discharge' as parameter,
             nullif(iv."141284_00060", '') as primary_value,
             nullif(iv."227924_00060", '') as secondary_value,
             'cfs' as unit,
             iv."141284_00060_cd" as primary_value_flag,
             iv."227924_00060_cd" as secondary_value_flag
         from NWIS_IV_08156675 iv
         union
         select
             iv.datetime as date_time,
             iv.site_no as site_number,
             'Gage height' as parameter,
             nullif(iv."141283_00065", '') as primary_value,
             nullif(iv."224476_00065", '') as secondary_value,
             'ft' as unit,
             iv."141283_00065_cd" as primary_value_flag,
             iv."224476_00065_cd" as secondary_value_flag
         from NWIS_IV_08156675 iv
     ) inner join NWIS_Sites s on s.site_no = site_number
        order by date_time desc
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        return geopandas.read_file(filename=self.db_path, sql=self.query_str)


class DischargeTransformable(Transformable):
    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        df['primary_value'] = pandas.to_numeric(df['primary_value'], errors='coerce').fillna(0.0)
        df['secondary_value'] = pandas.to_numeric(df['secondary_value'], errors='coerce').fillna(0.0)
        df['date_time'] = pandas.to_datetime(df['date_time'])
        return geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(x=df["longitude"], y=df["latitude"],
                                                                            crs="EPSG:26914"))
