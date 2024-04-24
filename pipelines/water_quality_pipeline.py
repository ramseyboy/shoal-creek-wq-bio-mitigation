from __future__ import annotations

import os
from typing import Union

import geopandas
import pandas
import shapely
from sqlalchemy import create_engine

from config import PostgresReadWriteConfig
from pipelines import Queryable, Transformable

geopandas.options.io_engine = "pyogrio"


class WaterQualityQueryable(Queryable):

    def __init__(self):
        self.conn_str = PostgresReadWriteConfig().__str__()
        self.engine = create_engine(self.conn_str)

        self.query_str = """
        select 
                sample_date_time::timestamp as sample_date_time,
               "parameter",
               latitude,
               longitude,
               organization_name,
               sample_location_id,
               sample_location,
               sample_type,
               value::float as value,
               unit,
               sample_status,
               geometry
        from (select
               (wq."ActivityStartDate" || 'T' ||
                (case when coalesce(wq."ActivityStartTime/Time", '') = ''
                          then '00:00:00'
                      else wq."ActivityStartTime/Time"
                    end)) as sample_date_time,
               wq."CharacteristicName" as "parameter",
               coalesce(NULLIF(wq."ActivityLocation/LatitudeMeasure",''), s.dec_lat_va) as latitude,
               coalesce(NULLIF(wq."ActivityLocation/LongitudeMeasure", ''), s.dec_long_va) as longitude,
               wq."OrganizationFormalName" as organization_name,
               wq."MonitoringLocationIdentifier" as sample_location_id,
               coalesce(NULLIF(wq."MonitoringLocationName",''), wq."MonitoringLocationIdentifier") as sample_location,
               wq."ActivityTypeCode" as sample_type,
               wq."ResultMeasureValue" as value,
               wq."ResultMeasure/MeasureUnitCode" as unit,
               wq."ResultStatusIdentifier" as sample_status,
               ST_Transform(ST_SetSRID(ST_MakePoint((coalesce(NULLIF(wq."ActivityLocation/LongitudeMeasure", ''), s.dec_long_va))::float, (coalesce(NULLIF(wq."ActivityLocation/LatitudeMeasure",''), s.dec_lat_va))::float), 4326), 26914) as geometry
        from staging."WaterQuality_NWIS_EPA" as wq
                 left outer join staging."NWIS_Sites" s on s.site_no = substr(wq."MonitoringLocationIdentifier", 6)
        where coalesce(NULLIF(wq."ActivityLocation/LatitudeMeasure",''), s.dec_lat_va) is not null
        and nullif(wq."ResultMeasureValue", '') is not null
        union
        select 
            ((substr(wq."SAMPLE_DATE", 7, 4) || '-' ||
                substr(wq."SAMPLE_DATE", 1, 2) || '-' ||
                substr(wq."SAMPLE_DATE", 4, 2)) || 'T' ||
               substr(wq."SAMPLE_DATE", 12, 8)) as sample_date_time,
               wq."PARAMETER" as "parameter",
               wq."LAT_DD_WGS84" as latitude,
               wq."LON_DD_WGS84" as longitude,
               'City of Austin' as organization_name,
               coalesce(NULLIF(wq."SAMPLE_ID",''), wq."SITE_NAME") as sample_location_id,
               wq."SITE_NAME" as sample_location,
               wq."METHOD" as sample_type,
               wq."RESULT" as value,
               wq."UNIT" as unit,
               'Final' as sample_status,
               ST_Transform(ST_SetSRID(ST_MakePoint(wq."LON_DD_WGS84"::float, wq."LAT_DD_WGS84"::float), 4326), 26914) as geometry
        from staging."WaterQuality_COA" as wq
        where coalesce(wq."LAT_DD_WGS84", '') != '' and wq."LAT_DD_WGS84" != '0'
          and wq."MEDIUM" = 'Surface Water'
          and nullif(wq."RESULT", '') is not null
        order by sample_date_time)
        where value ~ '^[0-9\.]+$' and ST_Within(geometry, (select geometry from public.watershed limit 1))
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        return geopandas.read_postgis(
            sql=self.query_str,
            con=self.engine, geom_col='geometry', crs="EPSG:26914")


class WaterQualityTransformable(Transformable):

    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        return df
