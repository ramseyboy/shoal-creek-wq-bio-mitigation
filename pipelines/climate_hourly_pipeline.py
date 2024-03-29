import os
from typing import Union

import geopandas
import pandas
from shapely.wkt import loads

from pipelines import Transformable, Queryable

geopandas.options.io_engine = "pyogrio"


class ClimateHourlyQueryable(Queryable):

    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/staging.gpkg")

        self.query_str = """
            select hex(date_time || station_number || parameter) as ckey,
                   date_time,
                   '-97.7604' as longitude, 
                   '30.3208' as latitude,
                   station_number,
                   nullif(value, '') as value,
                   parameter
            from (SELECT r.DATE                      as date_time,
                         r.STATION                   as station_number,
                         r.HourlyDewPointTemperature as value,
                         'Dew Point Temperature'     as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  union
                  SELECT r.DATE                     as date_time,
                         r.STATION                  as station_number,
                         r.HourlyWetBulbTemperature as value,
                         'Wet Bulb Temperature'     as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  union
                  SELECT r.DATE                     as date_time,
                         r.STATION                  as station_number,
                         r.HourlyDryBulbTemperature as value,
                         'Dry Bulb Temperature'     as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  union
                  SELECT r.DATE                   as date_time,
                         r.STATION                as station_number,
                         r.HourlyRelativeHumidity as value,
                         'Relative Humidity'      as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  union
                  SELECT r.DATE                   as date_time,
                         r.STATION                as station_number,
                         r.HourlyRelativeHumidity as value,
                         'Relative Humidity'      as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  union
                  SELECT r.DATE                   as date_time,
                         r.STATION                as station_number,
                         r.HourlyPrecipitation as value,
                         'Precipitation'      as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  union
                  SELECT r.DATE                   as date_time,
                         r.STATION                as station_number,
                         r.HourlyVisibility as value,
                         'Hourly Visibility'      as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  union
                  SELECT r.DATE                   as date_time,
                         r.STATION                as station_number,
                         r.HourlyWindDirection as value,
                         'Wind Direction'      as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  union
                  SELECT r.DATE                   as date_time,
                         r.STATION                as station_number,
                         r.HourlyWindSpeed as value,
                         'Wind Speed'      as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  union
                  SELECT r.DATE                   as date_time,
                         r.STATION                as station_number,
                         r.HourlyWindGustSpeed as value,
                         'Wind Gust Speed'      as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  union
                  SELECT r.DATE                   as date_time,
                         r.STATION                as station_number,
                         r.HourlySeaLevelPressure as value,
                         'Sea Level Pressure'      as parameter,
                         r.REPORT_TYPE,
                         r.SOURCE
                  FROM NOAA_CM_Hourly r
                  )
            where REPORT_TYPE == 'FM-15'
              and SOURCE = '343'
              and value is not null
          order by date_time
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        return geopandas.read_file(filename=self.db_path, sql=self.query_str)


class ClimateHourlyTransformable(Transformable):
    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        df = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(x=df["longitude"], y=df["latitude"],
                                                                               crs="EPSG:4326"))
        df['date_time'] = pandas.to_datetime(df['date_time'])

        df = df[
            (df.value.notnull()) &
            (pandas.to_numeric(df.value, errors='coerce').notnull()) &
            (pandas.to_numeric(df.value, errors='coerce') > 0.0)
            ]
        df.value = df.value.astype(float)

        climate = df.to_crs(crs="EPSG:26914")
        return climate
