import os
from typing import Union
from config import PostgresReadWriteConfig
from sqlalchemy import create_engine
import geopandas
import pandas

from pipelines import Transformable, Queryable


class ClimateDailyQueryable(Queryable):

    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/staging.gpkg")

        self.query_str = """
            select encode((date_time || station_number || "parameter")::bytea, 'hex') as ckey,
                   date_time,
                   '-97.7604' as longitude, 
                   '30.3208' as latitude,
                   station_number,
                   nullif(value, '') as value,
                   "parameter"
            from (SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailyAverageDewPointTemperature" as value,
                         'Dew Point Temperature'     as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  union
                  SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailyAverageWetBulbTemperature" as value,
                         'Wet Bulb Temperature'     as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  union
                  SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailyAverageDryBulbTemperature" as value,
                         'Dry Bulb Temperature'     as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  union
                  SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailyAverageRelativeHumidity" as value,
                         'Relative Humidity'      as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  union
                  SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailyPrecipitation" as value,
                         'Precipitation'      as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  union
                  SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailyAverageWindSpeed" as value,
                         'Average Wind Speed'      as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  union
                  SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailySustainedWindDirection" as value,
                         'Sustained Wind Direction'      as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  union
                  SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailySustainedWindSpeed" as value,
                         'Sustained Wind Speed'      as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  union
                  SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailyPeakWindDirection" as value,
                         'Peak Wind Direction'      as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  union
                  SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailyPeakWindSpeed" as value,
                         'Peak Wind Speed'      as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  union
                  SELECT r."DATE"                      as date_time,
                         r."STATION"                 as station_number,
                         r."DailyAverageSeaLevelPressure" as value,
                         'Sea Level Pressure'      as "parameter",
                         r."REPORT_TYPE"
                  FROM staging."NOAA_CM_Hourly" r
                  )
            where "REPORT_TYPE" = 'SOD'
              and value is not null
          order by date_time
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        conn_str = PostgresReadWriteConfig().__str__()
        engine = create_engine(conn_str)
        return pandas.read_sql_query(sql=self.query_str, con=engine)


class ClimateDailyTransformable(Transformable):
    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        df = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(x=df["longitude"], y=df["latitude"],
                                                                            crs="EPSG:4326"))
        df['date_time'] = pandas.to_datetime(df['date_time'])
        df['value'] = pandas.to_numeric(df['value'], errors='coerce')

        climate = df.to_crs(crs="EPSG:26914")
        return climate
