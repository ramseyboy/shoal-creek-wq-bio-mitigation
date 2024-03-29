from __future__ import annotations

import os
from typing import Union

import geopandas
import pandas
import shapely

from pipelines import Queryable, Transformable

geopandas.options.io_engine = "pyogrio"


class WaterQualityQueryable(Queryable):

    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/staging.gpkg")

        self.query_str = """
        select hex(CAST(strftime('%s', sample_date_time) AS INT) || parameter || sample_location_id || source_fid) as ckey
                , * from (select
                wq.fid as source_fid,
               (wq.ActivityStartDate || 'T' ||
                (case when coalesce(wq."ActivityStartTime/Time", '') = ''
                          then '00:00:00'
                      else wq."ActivityStartTime/Time"
                    end)) as sample_date_time,
               wq.CharacteristicName as parameter,
               coalesce(NULLIF(wq."ActivityLocation/LatitudeMeasure",''), s.dec_lat_va) as latitude,
               coalesce(NULLIF(wq."ActivityLocation/LongitudeMeasure", ''), s.dec_long_va) as longitude,
               wq.OrganizationFormalName as organization_name,
               wq.MonitoringLocationIdentifier as sample_location_id,
               coalesce(NULLIF(wq.MonitoringLocationName,''), wq.MonitoringLocationIdentifier) as sample_location,
               wq.ActivityTypeCode as sample_type,
               wq.ResultMeasureValue as value,
               wq."ResultMeasure/MeasureUnitCode" as unit,
               wq.ResultStatusIdentifier as sample_status 
        from WaterQuality_NWIS_EPA as wq
                 left outer join NWIS_Sites s on s.site_no = substr(wq.MonitoringLocationIdentifier, 6)
        where coalesce(NULLIF(wq."ActivityLocation/LatitudeMeasure",''), s.dec_lat_va) is not null
        and nullif(value, '') is not null
        union
        select 
        wq.fid as source_fid,
        (substr(wq.SAMPLE_DATE, 7, 4) || '-' ||
                substr(wq.SAMPLE_DATE, 1, 2) || '-' ||
                substr(wq.SAMPLE_DATE, 4, 2)) || 'T' ||
               substr(wq.SAMPLE_DATE, 12, 8) as sample_date_time,
               wq.PARAMETER as parameter,
               wq.LAT_DD_WGS84 as latitude,
               wq.LON_DD_WGS84 as longitude,
               'City of Austin' as irganization_name,
               coalesce(NULLIF(wq.SAMPLE_ID,''), wq.SITE_NAME) as sample_location_id,
               wq.SITE_NAME as sample_location,
               wq.METHOD as sample_type,
               wq.RESULT as value,
               wq.UNIT as unit,
               'Final' as sample_status
        from WaterQuality_COA as wq
        where coalesce(wq.LAT_DD_WGS84, '') != ''
          and MEDIUM = 'Surface Water'
          and nullif(value, '') is not null
        order by sample_date_time)
        where value GLOB '*[0-9]*'
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        return geopandas.read_file(filename=self.db_path, sql=self.query_str)


class WaterQualityTransformable(Transformable):

    def __init__(self, boundary_geometry: shapely.Geometry):
        self.boundary_geometry = boundary_geometry

    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        usgs = df[(df["sample_location_id"].str.contains('USGS-08156800')) |
                  (df["sample_location_id"].str.contains('USGS-08156675'))]
        usgs = geopandas.GeoDataFrame(usgs, geometry=geopandas.points_from_xy(x=usgs["longitude"], y=usgs["latitude"],
                                                                              crs="EPSG:26914"))

        coa = df[~df["sample_location_id"].str.contains('USGS')]
        coa = geopandas.GeoDataFrame(coa,
                                        geometry=geopandas.points_from_xy(x=coa["longitude"], y=coa["latitude"],
                                                                          crs="EPSG:4326"))
        coa = coa.to_crs(crs="EPSG:26914")
        coa = coa[coa.within(self.boundary_geometry)]

        water_quality = geopandas.GeoDataFrame(pandas.concat([usgs, coa]), crs="EPSG:26914")

        water_quality['sample_date_time'] = pandas.to_datetime(water_quality['sample_date_time'])
        water_quality['value'] = pandas.to_numeric(df['value'])

        return water_quality
